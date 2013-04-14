package DR::TarantoolQueue;
use DR::Tarantool ();
use utf8;
use strict;
use warnings;
use Mouse;
use Carp;
use JSON::XS;
require DR::TarantoolQueue::Task;
$Carp::Internal{ (__PACKAGE__) }++;

our $VERSION = '0.11';

=head1 NAME

DR::TarantoolQueue - client for tarantool's queue


=head1 SYNOPSIS

    my $queue = DR::TarantoolQueue->new(
        host    => 'tarantool.host',
        port    => 33014,
        tube    => 'request_queue',
        space   => 11,

        connect_opts => {   # see perldoc DR::Tarantool
            reconnect_period    => 1,
            reconnect_always    => 1
        }
    );


    # put empty task into queue with name 'request_queue'
    my $task = $queue->put;

    my $task = $queue->put(data => [ 1, 2, 3 ]);

    printf "task.id = %s\n", $task->id;

=head2 DESCRIPTION

The module contains sync and async (coro) driver for tarantool queue.

=head1 ATTRIBUTES

=head2 host (ro) & port (ro)

Tarantool's parameters.

=head2 connect_opts (ro)

Additional options for L<DR::Tarantool>. HashRef.

=head2 coro (ro)

If B<true> (default) the driver will use L<Coro> tarantool's driver,
otherwise the driver will use sync driver.

=head2 ttl (rw)

Default B<ttl> for tasks.

=head2 ttr (rw)

Default B<ttr> for tasks.

=head2 pri (rw)

Default B<pri> for tasks.

=head2 delay (rw)

Default B<delay> for tasks.

=head2 space (rw)

Default B<space> for tasks.

=head2 tube (rw)

Default B<tube> for tasks.


=head2 defaults

Defaults for queues. B<HashRef>. Key is tube name. Value is a hash with
the following fields:

=over

=item ttl

=item ttr

=item delay

=item pri

=back

Methods L</put> (L</urgent>) use these parameters if they
are absent (otherwise it uses the same global attributes).

=cut

has host    => (is => 'ro', isa => 'Str',   required => 1);
has port    => (is => 'ro', isa => 'Str',   required => 1);
has coro    => (is => 'ro', isa => 'Bool',  default  => 1);

has ttl     => (is => 'rw', isa => 'Num|Undef');
has ttr     => (is => 'rw', isa => 'Num|Undef');
has pri     => (is => 'rw', isa => 'Num|Undef');
has delay   => (is => 'rw', isa => 'Num|Undef');
has space   => (is => 'rw', isa => 'Str|Undef');
has tube    => (is => 'rw', isa => 'Str|Undef');
with 'DR::TarantoolQueue::JSE';
has connect_opts => (is => 'ro', isa => 'HashRef', default => sub {{}});

has defaults => (is => 'ro', isa => 'HashRef', default => sub {{}});

sub tnt {
    my ($self) = @_;

    unless ($self->coro) {
        return $self->{tnt} if $self->{tnt};
        return $self->{tnt} = DR::Tarantool::tarantool
            port => $self->port,
            host => $self->host,
            spaces => {},
            %{ $self->connect_opts }
        ;
    }

    require Coro;
    return $self->{tnt} if $self->{tnt};
    if ($self->{tnt_waiter}) {
        push @{ $self->{tnt_waiter} } => $Coro::current;
        Coro::schedule();
        return $self->{tnt};
    }
    $self->{tnt_waiter} = [];
    $self->{tnt} = DR::Tarantool::coro_tarantool
        port => $self->port,
        host => $self->host,
        spaces => {},
        %{ $self->connect_opts }
    ;
    $_->ready for @{ $self->{tnt_waiter} };
    delete $self->{tnt_waiter};
    return $self->{tnt};
}


sub _check_opts($@) {
    my $h = shift;
    my %can = map { ($_ => 1) } @_;

    for (keys %$h) {
        next if $can{$_};
        croak 'unknown option: ' . $_;
    }
}

sub _producer {
    my ($self, $method, $o) = @_;

    _check_opts $o, qw(space tube delay ttl ttr pri data);

    my $space = $o->{space};
    $space = $self->space unless defined $space;
    croak 'space was not defined' unless defined $space;

    my $tube = $o->{tube};
    $tube  = $self->tube unless defined $tube;
    croak 'tube was not defined' unless defined $tube;

    my ($ttl, $ttr, $pri, $delay);

    for ([\$ttl, 'ttl'], [\$delay, 'delay'], [\$ttr, 'ttr'], [\$pri, 'pri']) {
        my $rv = $_->[0];
        my $n = $_->[1];

        if (exists $o->{$n}) {
            $$rv = $o->{$n};
        } else {
            if (exists $self->defaults->{ $tube }) {
                if (exists $self->defaults->{ $tube }{ $n }) {
                    $$rv = $self->defaults->{ $tube }{ $n };
                } else {
                    $$rv = $self->$n;
                }
            } else {
                $$rv = $self->$n;
            }
        }
        $$rv ||= 0;

    }


    my $tuple = $self->tnt->call_lua(
        "queue.$method" => [
            $space,
            $tube,
            $delay,
            $ttl,
            $ttr,
            $pri,
            $self->jse->encode($o->{data})
        ]
    );

    return DR::TarantoolQueue::Task->tuple($tuple, $space, $self);
}

=head1 METHODS

=head2 new

    my $q = DR::TarantoolQueue->new(host => 'abc.com', port => 123);

Creates new queue(s) accessor.

=cut

=head2 dig

    $q->dig(task => $task);
    $task->dig; # the same

    $q->dig(id => $task->id);
    $q->dig(id => $task->id, space => $task->space);

'Dig up' a buried task. Checks, that the task is buried.
The task status is changed to ready.

=head2 unbury

Is a synonym of L</dig>.


=head2 delete

    $q->delete(task => $task);
    $task->delete; # the same

    $q->delete(id => $task->id);
    $q->delete(id => $task->id, space => $task->space);

Delete a task from the queue (regardless of task state or status).

=head2 peek

    $q->peek(task => $task);
    $task->peek; # the same

    $q->peek(id => $task->id);
    $q->peek(id => $task->id, space => $task->space);

Return a task by task id.


=head2 statistics

    my $s = $q->statistics;
    my $s = $q->statistics(space => 123);
    my $s = $q->statistics(space => 123, tube => 'abc');
    my $s = DR::TarantoolQueue->statistics(space => 123);
    my $s = DR::TarantoolQueue->statistics(space => 123, tube => 'abc');

Return queue module statistics, since server start.
The statistics is broken down by queue id.
Only queues on which there was some activity are
included in the output.


=cut

sub statistics {
    my ($self, %o) = @_;
    _check_opts \%o, qw(space tube);
    unless (exists $o{space}) {
        $o{space} = $self->space if ref $self;
    }
    unless (exists $o{tube}) {
        $o{tube} = $self->tube if ref $self;
    }

    croak 'space was not defined'
        if defined $o{tube} and !defined $o{space};

    my $raw = $self->tnt->call_lua(
        "queue.statistics" => [
            defined($o{space}) ? $o{space} : (),
            defined($o{tube}) ? $o{tube} : ()
        ]
    )->raw;
    return { @$raw };
}


=head2 get_meta

Task was processed (and will be deleted after the call).

    my $m = $q->get_meta(task => $task);
    my $m = $q->get_meta(id => $task->id);

Returns a hashref with fields:


=over

=item id

task id

=item tube

queue id

=item status

task status

=item event

time of the next important event in task life time, for example,
when ttl or ttr expires, in microseconds since start of the UNIX epoch.

=item ipri

internal value of the task priority

=item pri

task priority as set when the task was added to the queue

=item cid

consumer id, of the consumer which took the task (only if the task is taken)

=item created

time when the task was created (microseconds since start of the UNIX epoch)

=item ttl

task time to live (microseconds)

=item ttr

task time to run (microseconds)

=item cbury

how many times the task was buried

=item ctaken

how many times the task was taken

=item now

time recorded when the meta was called

=back

=cut

sub get_meta {
    my ($self, %o) = @_;
    _check_opts \%o, qw(task id space);
    croak 'task was not defined' unless $o{task} or $o{id};

    my ($id, $space, $tube);
    if ($o{task}) {
        ($id, $space, $tube) = ($o{task}->id,
            $o{task}->space, $o{task}->tube);
    } else {
        ($id, $space, $tube) = @o{'id', 'space', 'tube'};
        $space = $self->space unless defined $o{space};
        croak 'space is not defined' unless defined $space;
        $tube = $self->tube unless defined $tube;
    }


    my $fields = [
        {   name => 'id',       type => 'STR'       },
        {   name => 'tube',     type => 'STR'       },
        {   name => 'status',   type => 'STR'       },
        {   name => 'event',    type => 'NUM64'     },
        {   name => 'ipri',     type => 'STR',      },
        {   name => 'pri',      type => 'STR',      },
        {   name => 'cid',      type => 'NUM',      },
        {   name => 'created',  type => 'NUM64',    },
        {   name => 'ttl',      type => 'NUM64'     },
        {   name => 'ttr',      type => 'NUM64'     },
        {   name => 'cbury',    type => 'NUM'       },
        {   name => 'ctaken',   type => 'NUM'       },
        {   name => 'now',      type => 'NUM64'     },
    ];
    my $tuple = $self->tnt->call_lua(
        "queue.meta" => [ $space, $id ], fields => $fields
    )->raw;


    return { map { ( $fields->[$_]{name}, $tuple->[$_] ) } 0 .. $#$fields };
}




=head1 Producer methods

=head2 put

    $q->put;
    $q->put(data => { 1 => 2 });
    $q->put(space => 1, tube => 'abc',
            delay => 10, ttl => 3600,
            ttr => 60, pri => 10, data => [ 3, 4, 5 ]);
    $q->put(data => 'string');


Enqueue a task. Returns new L<task|DR::TarantoolQueue::Task> object.
The list of fields with task data (C<< data => ... >>) is optional.


If 'B<space>' and (or) 'B<tube>' aren't defined the method
will try to use them from L<queue|DR::TarantoolQueue/new> object.

=cut

sub put {
    my ($self, %opts) = @_;
    return $self->_producer(put => \%opts);
}

=head2 urgent

Enqueue a task. The task will get the highest priority.
If delay is not zero, the function is equivalent to
L<put|DR::TarantoolQueue/put>.

=cut

sub urgent {
    my ($self, %opts) = @_;
    return $self->_producer(urgent => \%opts);
}


=head1 Consumer methods

=head2 take

    my $task = $q->take;
    my $task = $q->take(timeout => 0.5);
    my $task = $q->take(space => 1, tube => 'requests, timeout => 20);

If there are tasks in the queue ready for execution,
take the highest-priority task. Otherwise, wait for
a ready task to appear in the queue, and, as soon as
it appears, mark it as taken and return to the consumer.
If there is a timeout, and the task doesn't appear until
the timeout expires, returns B<undef>. If timeout is not
given, waits indefinitely.

All the time while the consumer is working on a task,
it must keep the connection to the server open. If a
connection disappears while the consumer is still
working on a task, the task is put back on the ready list.

=cut

sub take {
    my ($self, %o) = @_;
    _check_opts \%o, qw(space tube timeout);
    $o{space} = $self->space unless defined $o{space};
    croak 'space was not defined' unless defined $o{space};
    $o{tube} = $self->tube unless defined $o{tube};
    croak 'tube was not defined' unless defined $o{tube};
    $o{timeout} ||= 0;


    my $tuple = $self->tnt->call_lua(
        'queue.take' => [
            $o{space},
            $o{tube},
            $o{timeout}
        ]
    );


    return DR::TarantoolQueue::Task->tuple($tuple, $o{space}, $self);
}


=head2 ack

    $q->ack(task => $task);
    $task->ack; # the same

    $q->ack(id => $task->id);
    $q->ack(space => $task->space, id => $task->id);


Confirm completion of a task. Before marking a task as
complete, this function verifies that:

=over

=item *

the task is taken

=item *

the consumer that is confirming the task is the one which took it

=back

Consumer identity is established using a session identifier.
In other words, the task must be confirmed by the same connection
which took it. If verification fails, the function returns an error.

On success, deletes the task from the queue. Throws an exception otherwise.


=head2 requeue

    $q->requeue(task => $task);
    $task->requeue; # the same

    $q->requeue(id => $task->id);
    $q->requeue(id => $task->id, space => $task->space);

Return a task to the queue, the task is not executed.
Puts the task at the end of the queue, so that it's executed
only after all existing tasks in the queue are executed.


=head2 bury

    $q->bury(task => $task);
    $task->bury; # the same

    $q->bury(id => $task->id);
    $q->bury(id => $task->id, space => $task->space);

Mark a task as B<buried>. This special status excludes the task
from the active list, until it's dug up. This function is useful
when several attempts to execute a task lead to a failure. Buried
tasks can be monitored by the queue owner, and treated specially.


=cut

for my $m (qw(ack requeue bury dig unbury delete peek)) {
    no strict 'refs';
    next if *{ __PACKAGE__ . "::$m" }{CODE};
    *{ __PACKAGE__ . "::$m" } = sub {
        my ($self, %o) = @_;
        _check_opts \%o, qw(task id space);
        croak 'task was not defined' unless $o{task} or $o{id};

        my ($id, $space);
        if ($o{task}) {
            ($id, $space) = ($o{task}->id, $o{task}->space);
        } else {
            ($id, $space) = @o{'id', 'space'};
            $space = $self->space unless defined $o{space};
            croak 'space is not defined' unless defined $space;
        }

        my $tuple = $self->tnt->call_lua( "queue.$m" => [ $space, $id ] );
        my $task = DR::TarantoolQueue::Task->tuple($tuple, $space, $self);

        if ($m eq 'delete') {
            $task->_set_status('removed');
        } elsif ($m eq 'ack') {
            $task->_set_status('ack(removed)');
        }
        $task;
    }
}


=head2 release

    $q->release(task => $task);
    $task->release; # the same

    $q->release(id => $task->id, space => $task->space);
    $q->release(task => $task, delay => 10); # delay the task
    $q->release(task => $task, ttl => 3600); # append task's ttl

Return a task back to the queue: the task is not executed.
Additionally, a new time to live and re-execution delay can be provided.

=cut

sub release {
    my ($self, %o) = @_;
    _check_opts \%o, qw(task id space ttl delay);
    $o{delay} ||= 0;
    my ($id, $space);
    if ($o{task}) {
        ($id, $space) = ($o{task}->id, $o{task}->space);
    } else {
        ($id, $space) = @o{'id', 'space'};
        $space = $self->space unless defined $o{space};
        croak 'space is not defined' unless defined $space;
    }
    my $tuple = $self->tnt->call_lua('queue.release' =>
        [ $space, $id, $o{delay}, $o{ttl} || () ]
    );
    return DR::TarantoolQueue::Task->tuple($tuple, $space, $self);
}



=head2 done

    $q->done(task => $task, data => { result => '123' });
    $task->done(data => { result => '123' }); # the same
    $q->done(id => $task->id, space => $task->space);

Mark a task as complete (done), but don't delete it. Replaces task
data with the supplied B<data>.

=cut

sub done {
    my ($self, %o) = @_;
    _check_opts \%o, qw(task id space data);
    my ($id, $space);
    if ($o{task}) {
        ($id, $space) = ($o{task}->id, $o{task}->space);
    } else {
        ($id, $space) = @o{'id', 'space'};
        $space = $self->space unless defined $o{space};
        croak 'space is not defined' unless defined $space;
    }
    my $tuple = $self->tnt->call_lua('queue.done' =>
        [ $space, $id, $self->jse->encode($o{data}) ]
    );
    return DR::TarantoolQueue::Task->tuple($tuple, $space, $self);
}


=head1 COPYRIGHT AND LICENCE

 Copyright (C) 2012 by Dmitry E. Oboukhov <unera@debian.org>
 Copyright (C) 2012 by Roman V. Nikolaev <rshadow@rambler.ru>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

__PACKAGE__->meta->make_immutable();
