#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More;
use constant PLAN => 79;

BEGIN {
    system 'which tarantool_box >/dev/null 2>&1';
    if ($? == 0) {
        plan tests    => PLAN;
    } else {
        plan skip_all => 'tarantool_box not found';
    }
}

use Encode qw(decode encode);
use Cwd 'cwd';
use File::Spec::Functions 'catfile';
use feature 'state';



BEGIN {
    # Подготовка объекта тестирования для работы с utf8
    my $builder = Test::More->builder;
    binmode $builder->output,         ":utf8";
    binmode $builder->failure_output, ":utf8";
    binmode $builder->todo_output,    ":utf8";

    use_ok 'Coro';
    use_ok 'DR::Tarantool', ':all';
    use_ok 'DR::Tarantool::StartTest';
    use_ok 'Time::HiRes', 'time';
    use_ok 'Coro::AnyEvent';
}
my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'tarantool.cfg'),
    script_dir  => catfile(cwd)
);

$SIG{INT} = sub {
    note $t->log if $ENV{DEBUG};
    $t->kill('KILL');
    exit 2;
};

our $tnt;
sub tnt {
    unless($tnt) {
        $tnt = eval {
            coro_tarantool
                host => 'localhost',
                port => $t->primary_port,
                spaces => {
                    0   => {
                        name            => 'queue',
                        default_type    => 'STR',
                        fields          => [
                            qw(uuid tube status),
                            {
                                type => 'NUM64',
                                name => 'event'
                            },
                            {
                                type => 'STR',
                                name => 'ipri'
                            },
                            {
                                type => 'STR',
                                name => 'pri'
                            },
                            {
                                type => 'NUM',
                                name => 'cid',
                            },
                            {
                                type => 'NUM64',
                                name => 'started'
                            },
                            {
                                type => 'NUM64',
                                name => 'ttl',
                            },
                            {
                                type => 'NUM64',
                                name => 'ttr',
                            },
                            {
                                type => 'NUM64',
                                name => 'bury'
                            },
                            {
                                type => 'NUM64',
                                name => 'taken'
                            },
                            'task',
                        ],
                        indexes => {
                            0 => 'uuid',
                            1 => {
                                name => 'event',
                                fields => [qw(tube status event pri)]
                            }
                        }
                    }
                },
            };
            note $t->log unless $tnt;
    }
    $tnt;
};

ok tnt->ping, 'ping tarantool';
diag $t->log unless
    ok $t->started, 'Tarantool was started';
diag $t->log unless
    ok eval { tnt }, 'Client connected to';

my $sno = tnt->space('queue')->number;

my $task1 = tnt->call_lua('queue.put',
    [
        $sno,
        'tube_name',
        0,
        10,
        20,
        30,
        'task', 1 .. 10
    ]
)->raw;



is tnt->call_lua('queue.meta', [ $sno, $task1->[0] ])->raw(2), 'ready',
    'task1 is ready';

my $meta =  tnt->call_lua('queue.meta', [ $sno, $task1->[0] ],
    fields => [
        { type => 'STR', name => 'id' },
        { type => 'STR', name => 'tube' },
        { type => 'STR', name => 'status' },
        { type => 'NUM64', name => 'event' },
        { type => 'STR', name => 'ipri' },
        { type => 'STR', name => 'pri' },
        { type => 'NUM', name => 'cid' },
        { type => 'NUM64', name => 'created' },
        { type => 'NUM64', name => 'ttl' },
        { type => 'NUM64', name => 'ttr' },
        { type => 'NUM', name => 'cbury' },
        { type => 'NUM', name => 'ctaken' },
        { type => 'NUM64', name => 'now' },
    ]
);

cmp_ok $meta->now, '<', $meta->ttl + $meta->created, 'task is alive';
is $meta->ttl, 10000000, 'ttl';
is $meta->ttr, 20000000, 'ttr';

is_deeply $task1, [ $task1->[0], 'tube_name', 'ready', 'task', 1 .. 10 ],
    'task 1';


my $started = time;
my $task2 = tnt->call_lua('queue.put',
    [
        $sno,
        'tube_name',
        .5,
        10,
        20,
        30,
        'task', 10 .. 20
    ]
)->raw;

is tnt->call_lua('queue.meta', [ $sno, $task2->[0] ])->raw(2), 'delayed',
    'task2 is delayed';

is_deeply tnt->call_lua('queue.peek', [ $sno, $task2->[0] ])->raw, $task2,
    'task2.get';

is_deeply $task2, [ $task2->[0], 'tube_name',
    'delayed', 'task', 10 .. 20 ], 'task 2';

my $task1_t = tnt->call_lua('queue.take', [ $sno, 'tube_name', 5 ])->raw;
$task1->[2] = 'taken';
is_deeply $task1_t, $task1, 'task1 taken';
is tnt->call_lua('queue.meta', [ $sno, $task1->[0] ])->raw(2), 'taken',
    'task1 is taken';


my $task2_t = eval {tnt->call_lua('queue.take', [ $sno, 'tube_name', 5 ])->raw};
$task2->[2] = 'taken';
is_deeply $task2_t, $task2, 'task2 taken';
cmp_ok time - $started, '>=', .5, 'delay more than 0.5 second';
cmp_ok time - $started, '<=', .7, 'delay less than 0.7 second';

is_deeply tnt->call_lua('queue.peek', [ $sno, $task2->[0] ])->raw, $task2,
    'queue.peek';

my $task_ack = tnt->call_lua('queue.ack', [ $sno, $task2->[0] ])->raw;
is_deeply $task_ack, $task2, 'task was ack';


is_deeply scalar eval { tnt->call_lua('queue.peek', [ $sno, $task2->[0] ]) },
    undef, 'queue.peek';
like $@, qr{Task not found}, 'task not found';

$task_ack = eval { tnt->call_lua('queue.ack', [ $sno, $task2->[0] ]) };
like $@, qr{task not found}i, 'error message';
is $task_ack, undef, 'repeat ack';

$task1->[2] = 'ready';
is_deeply  tnt->call_lua('queue.release', [ $sno, $task1->[0] ])->raw, $task1,
    'task1 release';
is tnt->call_lua('queue.meta', [ $sno, $task1->[0] ])->raw(2), 'ready',
    'task1 is ready';

$task1_t = tnt->call_lua('queue.take', [ $sno, 'tube_name', 5 ])->raw;
$task1->[2] = 'taken';
is_deeply $task1_t, $task1, 'repeatly take task1';
$started = time;
$task1->[2] = 'delayed';
is_deeply  tnt->call_lua('queue.release', [ $sno, $task1->[0], .5 ])->raw,
    $task1, 'task1 release (delayed)';
is tnt->call_lua('queue.meta', [ $sno, $task1->[0] ])->raw(2), 'delayed',
    'task1 is delayed';
$task1_t = tnt->call_lua('queue.take', [ $sno, 'tube_name', 5 ])->raw;
cmp_ok time - $started, '>=', .5, 'take took more than 0.5 second';
cmp_ok time - $started, '<=', .7, 'take took less than 0.7 second';

is tnt->call_lua('queue.meta', [ $sno, $task1->[0] ])->raw(2), 'taken',
    'task1 is run';
$task1->[2] = 'taken';
is_deeply $task1_t, $task1, 'task1 is deeply';

is_deeply tnt->call_lua('queue.ack', [ $sno, $task1->[0]])->raw,
    $task1, 'task1.ack';


$task1 = tnt->call_lua('queue.put', 
    [
        $sno,
        'tube_name',
        0,                  # delay
        5,                  # ttl
        0.5,                # ttr
        30,                 # pri 
        'task', 30 .. 40
    ]
)->raw;

my $task1_m = tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue');

$task2 = tnt->call_lua('queue.urgent', 
    [
        $sno,
        'tube_name',
        0,                  # delay
        5,                  # ttl
        0.5,                # ttr
        30,                 # pri 
        'task', 40 .. 50
    ]
)->raw;

my $task3 = tnt->call_lua('queue.put', 
    [
        $sno,
        'tube_name',
        .5,                 # delay
        .1,                 # ttl
        0.1,                # ttr
        30,                 # pri 
        'task', 50 .. 60
    ]
)->raw;

my $task4 = tnt->call_lua('queue.put', 
    [
        $sno,
        'tube_name',
        0,                  # delay
        5,                  # ttl
        1,                  # ttr
        30,                 # pri 
        'task', 50 .. 60
    ]
)->raw;

my $task2_m = tnt->call_lua('queue.meta', [ $sno, $task2->[0] ], 'queue');

cmp_ok $task2_m->ipri, '<', $task1_m->ipri, 'ipri(task2) < ipri(task1)';

$task2_t = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;
$task1_t = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;
isnt $task1->[0], $task2->[0], "task1 and task2 aren't the same";

$task1->[2] = $task2->[2] = 'taken';
is_deeply $task1_t, $task1, 'task1 was fetched as urgent';
is_deeply $task2_t, $task2, 'task2 was fetched as usual';


for (1 .. 10) {
    Coro::AnyEvent::sleep .2;
    tnt->call_lua('queue.touch', [ $sno, $task1->[0] ])->raw;
}

$task1_m = tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue');
$task2_m = tnt->call_lua('queue.meta', [ $sno, $task2->[0] ], 'queue');

is $task1_m->status, 'taken', 'task1 is taken';
is $task2_m->status, 'ready', 'task2 is ready (by ttr)';

is scalar eval { tnt->call_lua('queue.peek', [ $sno, $task3->[0] ]) }, undef,
    'task3 was deleted by ttl';
like $@, qr{Task not found}, 'error string';

$task1->[2] = 'ready';
is_deeply tnt->call_lua('queue.requeue', [ $sno, $task1->[0] ])->raw,
    $task1, 'task1.requeue';

cmp_ok tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue')->ipri,
    '>',
    $task1_m->ipri,
    'requeue increase ipri';

$task1_m = tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue');
is $task1_m->status, 'ready', 'requeue sets status as ready';
is $task1_m->taken, 1, 'task has ctaken=1';

$task1 = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;
$task2 = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;
isnt $task1->[0], $task2->[0], 'task1 and task2 were taken';

$tnt->disconnect(sub {  });
$tnt = undef;

ok tnt->ping, 'new tarantool connection';

$task1_m = tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue');
$task2_m = tnt->call_lua('queue.meta', [ $sno, $task2->[0] ], 'queue');

is $task1_m->status, 'ready', 'task1 is ready (by dead consumer)';
is $task2_m->status, 'ready', 'task2 is ready (by dead consumer)';

$task1 = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;
$task2 = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;
isnt $task1->[0], $task2->[0], 'task1 and task2 were taken';

$task1_t = tnt->call_lua('queue.done', [ $sno, $task1->[0], 'abc' ])->raw;

is_deeply
    $task1_t,
    [ $task1->[0], 'tube_name', 'done', 'abc' ],
    'task1.done';

$task1_m = tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue');
is $task1_m->status, 'done', 'task1.status';

my %stat = @{ tnt->call_lua('queue.statistics', [])->raw };

is $stat{"space$sno.tube_name.ready_by_disconnect"}, 2,
    'cnt ready_by_disconnect';
is $stat{"space$sno.tube_name.ttl.total"}, 1,
    'cnt ttl.total';
is $stat{"space$sno.tube_name.tasks.buried"}, 0,
    'cnt tasks.buried';
is $stat{"space$sno.tube_name.tasks.ready"}, 1, 'cnt tasks.ready';
is $stat{"space$sno.tube_name.tasks.taken"}, 1, 'cnt tasks.taken';
is $stat{"space$sno.tube_name.tasks.done"}, 1, 'cnt tasks.done';


$task2_t = tnt->call_lua('queue.bury', [ $sno, $task2->[0] ])->raw;
$task2->[2] = 'buried';
is_deeply $task2_t, $task2, 'task2.bury';
$task2_m = tnt->call_lua('queue.meta', [ $sno, $task2->[0] ], 'queue');
is $task2_m->status, 'buried', 'task2.status';

is scalar eval { tnt->call_lua('queue.dig', [ $sno, $task1->[0] ])}, undef,
    'wrong task.dig';

$task2_t = tnt->call_lua('queue.dig', [ $sno, $task2->[0] ])->raw;
$task2->[2] = 'ready';
is_deeply $task2_t, $task2, 'task2.dig';
$task2_m = tnt->call_lua('queue.meta', [ $sno, $task2->[0] ], 'queue');
is $task2_m->status, 'ready', 'task2.status';


$task1 = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;
$task2 = tnt->call_lua('queue.take', [ $sno, 'tube_name' ])->raw;

# test restart function
is tnt->call_lua('queue.restart_check', [ $sno, 'tube_name' ])->raw(0),
    'already started',
    'call restart function';
tnt->call_lua('box.dostring', [ 'queue.restart = {}' ]);
is tnt->call_lua('queue.restart_check', [ $sno, 'tube_name' ])->raw(0),
    'starting',
    'call restart function';
is tnt->call_lua('queue.restart_check', [ $sno, 'tube_name' ])->raw(0),
    'already started',
    'call restart function';


isnt $task1->[0], $task2->[0], '2 tasks were taken';
$task1_t = tnt->call_lua('queue.bury', [ $sno, $task1->[0] ])->raw;
$task2_t = tnt->call_lua('queue.bury', [ $sno, $task2->[0] ])->raw;
$task1_m = tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue');
$task2_m = tnt->call_lua('queue.meta', [ $sno, $task2->[0] ], 'queue');
is $task1_m->status, 'buried', 'task1.status';
is $task2_m->status, 'buried', 'task2.status';


my $kicked = tnt->call_lua(
    'queue.kick', [ $sno, 'tube_name', 10 ],
    fields => [ { name => 'count', type => 'NUM' }]
)->raw(0);

is $kicked, 2, '2 tasks were kicked';

$task1_m = tnt->call_lua('queue.meta', [ $sno, $task1->[0] ], 'queue');
$task2_m = tnt->call_lua('queue.meta', [ $sno, $task2->[0] ], 'queue');
is $task1_m->status, 'ready', 'task1.status';
is $task2_m->status, 'ready', 'task2.status';


$task1 = tnt->call_lua('queue.put', [ $sno, 'pri', 0, 10, 10, 30, 'pri=30'])
    ->raw;
$task2 = tnt->call_lua('queue.put', [ $sno, 'pri', 0, 10, 10, 10, 'pri=10'])
    ->raw;
$task3 = tnt->call_lua('queue.put', [ $sno, 'pri', 0, 10, 10, 20, 'pri=20'])
    ->raw;


$task1->[2] = $task2->[2] = $task3->[2] = 'taken';

   $task1_t = tnt->call_lua('queue.take', [ $sno, 'pri' ])->raw;
my $task3_t = tnt->call_lua('queue.take', [ $sno, 'pri' ])->raw;
   $task2_t = tnt->call_lua('queue.take', [ $sno, 'pri' ])->raw;

is_deeply $task1_t, $task1, 'task1 (pri)';
is_deeply $task2_t, $task2, 'task2 (pri)';
is_deeply $task3_t, $task3, 'task3 (pri)';


END {
    note $t->log if $ENV{DEBUG};
}
