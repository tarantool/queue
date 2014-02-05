#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More;
use constant PLAN => 18;

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
#use feature 'state';



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
    timeout => 1,
    count   => 100
);

async {
    $wrk->run(sub { Coro::AnyEvent::sleep 1; die 123 });
};


my @tasks;
for (1 .. 100) {
    push @tasks => $q->put(tube => 'test_queue');
}

my @status;
push @status => $_->peek->status for @tasks;

cmp_ok 0, '<=', scalar grep({ $_ eq 'ready' } @status), 'some tasks are ready';
cmp_ok 0, '<', scalar grep({ $_ eq 'taken' } @status), 'some tasks are taken';
Coro::AnyEvent::sleep 0.5;

@status = ();
push @status => $_->peek->status for @tasks;

is scalar grep({ $_ eq 'ready' } @status), 0, 'no tasks are ready';
Coro::AnyEvent::sleep 1;

@status = ();
push @status => $_->peek->status for @tasks;
is scalar grep({ $_ eq 'buried' } @status), 100, 'all tasks are buried';

