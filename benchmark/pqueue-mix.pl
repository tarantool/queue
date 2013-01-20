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
use Coro::AnyEvent;
use DR::TarantoolQueue;

my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'tarantool.cfg'),
    script_dir  => catfile(cwd)
);

my $q = DR::TarantoolQueue->new(
    host    => '127.0.0.1',
    port    => $t->primary_port,
    space   => 0,
    tube    => 'test_tube'
);

use constant ITERATIONS => 1000;

my ($done, $total_time) = (0) x 4;
my $process = 1;

$SIG{INT} = $SIG{TERM} = sub {
    print "\nSIGING received\n";
    $t->kill unless $process;
    $process = 0;
};

while($process) {

    my (@f, %t);
    my $started = time;
    for (1 .. ITERATIONS) {
        push @f => async {
            my $task = $q->put(data => { num => rand });
            $t{ $task->id }++;
        };
        push @f => async {
            my $task = $q->take;
            $task->ack;
            $t{ $task->id }++;
        }
    }

    $_->join for @f; @f = ();

    $total_time += time - $started;
    $done += ITERATIONS;
    
    if (scalar keys %t != ITERATIONS) {
        print "Wrong results count\n";
        last;
    }
    if (ITERATIONS != grep { $_ == 2 } values %t) {
        print "Not all tasks were processed twice\n";
        last;
    }

    printf "Done %d sessions in %3.2f seconds (%d r/s, %f s/r)\n",
        $done,
        $total_time,
        $done / $total_time,
        $total_time / $done
    ;
}
