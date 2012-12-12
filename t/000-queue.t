#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 1;
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

    use_ok 'DR::Tarantool', ':all';
    use_ok 'DR::Tarantool::StartTest';
}
my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'config/db/tarantool.cfg'),
    script_dir  => catfile(cwd, 'config/db')
);

sub tnt {
    state $tnt;
    unless($tnt) {
        $tnt = coro_tarantool
            host => 'localhost',
            port => $t->primary_port,
            spaces => {
                0   => {
                    name            => 'queue',
                    default_type    => 'STR',
                    fields          => [
                        qw(uuid tube status),
                        {
                            type => 'NUM',
                            name => 'event'
                        },
                        {
                            type => 'NUM',
                            name => 'pri'
                        },
                        'cid',

                        {
                            type => 'NUM',
                            name => 'started'
                        },
                        {
                            type => 'NUM',
                            name => 'ttl',
                        },
                        {
                            type => 'NUM',
                            name => 'ttr',
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
    }
    $tnt;
};

diag $t->log unless
    ok $t->started, 'Tarantool was started';
diag $t->log unless
    ok eval { tnt }, 'Client connected to';

my $task = tnt->call_lua('queue.put',
    # queue.put = function(space, tube, delay, ttl, ttr, pri, ...)
    [
        tnt->space('queue')->number,
        'tube_name',
        0,
        10,
        20,
        30,
        'task',
    ], 'queue');
note explain $task->raw;

note explain tnt->call_lua('queue.statistic', [])->raw;

sleep .5;
note $t->log;
