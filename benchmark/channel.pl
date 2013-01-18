#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Encode qw(decode encode);
use Cwd 'cwd';
use File::Spec::Functions 'catfile';
use feature 'state';

use Coro;
use DR::Tarantool ':all';
use DR::Tarantool::StartTest;
use Time::HiRes 'time';
use Data::Dumper;

my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'tarantool.cfg'),
    script_dir  => catfile(cwd, 'benchmark')
);

sub tnt {
    our $tnt;
    unless(defined $tnt) {
        $tnt = coro_tarantool
            host => 'localhost',
            port => $t->primary_port,
            spaces => {}
        ;
    }
    return $tnt;
};

tnt->ping;

my (@f);
my $cnt = 2500;
my $done = 0;

my $started = time;

{
    my @tp = 1 .. $cnt;
    my @tg = 1 .. $cnt;
    while (@tp or @tg) {
        for (0 .. int rand 100) {
            next unless @tp;
            my $delay = rand;
            $delay = 0 if 20 > rand 100;
            push @f => async {
                eval {
                    tnt->call_lua('put', [ $delay,  shift @tp ]);
                    1;
                } or warn $@;
            };
            cede;
        }

        for (0 .. int rand 100) {
            next unless @tg;
            my $delay = rand;
            $delay = 0 if 20 > rand 100;
            push @f => async {
                eval {
                    tnt->call_lua('get', [ $delay,  shift @tg ]);
                    1;
                } or warn $@;
            };
            cede;
        }
    }

    $_->join for @f;
    @f = ();

    my $interval = time - $started;
    $done += $cnt;

    printf "Done %d pair of tasks in %3.2f second (%3.4f s/p and %d p/s)\n",
        $done,
        $interval,
        $interval / $done,
        $done / $interval
    ;
    redo;
}
