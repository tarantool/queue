#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More;
use constant PLAN => 30;

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

    use_ok 'DR::TarantoolQueue';
    use_ok 'Coro';
    use_ok 'DR::Tarantool', ':all';
    use_ok 'DR::Tarantool::StartTest';
    use_ok 'Time::HiRes', 'time';
    use_ok 'Coro::AnyEvent';
    use_ok 'DR::TarantoolQueue::Worker';
}
my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'tarantool.cfg'),
    script_dir  => catfile(cwd)
);


my $q = DR::TarantoolQueue->new(
    host    => '127.0.0.1',
    port    => $t->primary_port,
    space   => 0,
    tube    => 'test_queue',
);

isa_ok $q => 'DR::TarantoolQueue';

my (@f, @fh);
push @f => async {
    ok $q->tnt->ping, 'tnt ping';
    push @fh => fileno($q->tnt->_llc->{fh})
} for 1 .. 5;
$_->join for @f;
@f = ();
ok 5 == grep({ $_ == $fh[0] } @fh), 'connection established once';


my $wrk = DR::TarantoolQueue::Worker->new(
    queue   => $q,
    timeout => .5,
    count   => 10
);

async {
    $wrk->run(sub {});
};

my @tasks;
for (1 .. 31) {
    push @tasks => $q->put(data => $_);
}

Coro::AnyEvent::sleep 0.2;

for (@tasks) {
    $_ = eval { $q->peek(id => $_->id) };
}
is scalar grep({ $_ ~~ undef } @tasks), scalar @tasks, 'All tasks were ack';

is $wrk->stop, 0, 'workers were stopped';
@tasks = ();

async {
    $wrk->run(sub { $_[0]->release(delay => 1) });
};
@tasks = ();

for (1 .. 5) {
    push @tasks => $q->put(data => {task => $_});
}

Coro::AnyEvent::sleep .2;
for (@tasks) { is $_->peek->status, 'delayed', 'task was released' };

is $wrk->stop, 0, 'workers were stopped';

is $q->statistics->{'space0.test_queue.tasks.delayed'}, 5,
    '5 tasks are delayed';
is $q->statistics->{'space0.test_queue.tasks.buried'}, 0,
    '0 tasks are buried';

Coro::AnyEvent::sleep 1.1;

is $q->statistics->{'space0.test_queue.tasks.delayed'}, 0,
    '0 tasks are delayed';
is $q->statistics->{'space0.test_queue.tasks.ready'}, 5,
    '5 tasks are ready';
async {
    $wrk->run(sub { die 123 });
};
Coro::AnyEvent::sleep .2;
is $q->statistics->{'space0.test_queue.tasks.delayed'}, 0,
    '0 tasks are delayed';
is $q->statistics->{'space0.test_queue.tasks.buried'}, 5,
    '5 tasks are buried';
$wrk->stop;

my $restarted = 0;
$wrk->restart(sub { $restarted++ });
$wrk->restart_limit(15);
async {$wrk->run(sub {})};
$q->put(data => $_) for 1 .. 99;
$wrk->stop;

cmp_ok $restarted, '<=', 99 / $wrk->restart_limit, 'count of restarts';
cmp_ok $restarted, '>=', 99 / $wrk->restart_limit / 2, 'count of restarts';


END {
    note $t->log if $ENV{DEBUG};
}
