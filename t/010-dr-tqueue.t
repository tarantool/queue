#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More;
use constant PLAN => 39;

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
    use_ok 'DR::TarantoolQueue';
}
my $t = DR::Tarantool::StartTest->run(
    cfg         => catfile(cwd, 'config/db/tarantool.cfg'),
    script_dir  => catfile(cwd, 'config/db')
);


my $q = DR::TarantoolQueue->new(
    host    => '127.0.0.1',
    port    => $t->primary_port,
    space   => 0,
    tube    => 'test_queue',
);

my $qs = DR::TarantoolQueue->new(
    host    => '127.0.0.1',
    port    => $t->primary_port,
    space   => 0,
    tube    => 'test_queue',
    coro    => 0
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
ok $qs->tnt->ping, 'ping by sync client';

for ('put', 'urgent') {
    my $task1 = $q->$_;
    is_deeply $task1->data, undef, "$_()";
    like $task1->id, qr[^[0-9a-fA-F]{32}$], 'task1.id';
    my $task2 = $q->$_(data => { 1 => 2 });
    like $task2->id, qr[^[0-9a-fA-F]{32}$], 'task2.id';
    is_deeply $task2->data, { 1 => 2 }, "$_(data => hashref)";
    my $task3 = $q->$_(data => [ 3, 4, 'привет' ]);
    like $task3->id, qr[^[0-9a-fA-F]{32}$], 'task3.id';
    is_deeply $task3->data, [ 3, 4, 'привет' ], "$_(data => arrayref)";
}


my $task1_t = $q->take;
isa_ok $task1_t => 'DR::TarantoolQueue::Task';
my $task2_t = $q->take;
isa_ok $task2_t => 'DR::TarantoolQueue::Task';
my $task3_t = $q->take;
isa_ok $task3_t => 'DR::TarantoolQueue::Task';

isnt $task1_t->id, $task2_t->id, "task1 and task2 aren't the same";
isnt $task1_t->id, $task3_t->id, "task1 and task3 aren't the same";

isa_ok $task1_t->ack => 'DR::TarantoolQueue::Task', 'task1.ack';
isa_ok $q->ack(id => $task2_t->id), 'DR::TarantoolQueue::Task', 'task2.ack';

my $meta = $task3_t->get_meta;
isa_ok $meta => 'HASH', 'task3.meta.meta';
is $meta->{status}, 'taken', 'task3.meta.status';
is $meta->{ctaken}, 1, 'task3.meta.ctaken';
is $meta->{cbury}, 0, 'task3.meta.cbury';
is $meta->{tube}, 'test_queue', 'task3.meta.tube';
is $meta->{status}, $task3_t->status, 'task3.status';
