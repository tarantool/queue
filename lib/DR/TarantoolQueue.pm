package DR::TarantoolQueue;
use DR::Tarantool ();
use utf8;
use strict;
use warnings;
use Mouse;
use Carp;
use JSON::XS;
require DR::TarantoolQueue::Task;

our $VERSION = '0.02';

=head1 NAME

DR::TarantoolQueue - client for tarantool's queue


=head1 SYNOPSIS

    my $queue = DR::TarantoolQueue->new(
        host    => 'tarantool.host',
        port    => 33014,
        tube    => 'request_queue',
        space   => 11
    );


    # put empty task into queue with name 'request_queue'
    my $task = $queue->put;

    my $task = $queue->put(data => [ 1, 2, 3 ]);

    printf "task.id = %s\n", $task->id;

=head2 DESCRIPTION

The module contains sync (coro) and async driver fro tarantool queue.

=cut

has host    => (is => 'ro', isa => 'Str',   required => 1);
has port    => (is => 'ro', isa => 'Str',   required => 1);
has coro    => (is => 'ro', isa => 'Bool',  default  => 1);

has ttl     => (is => 'rw', isa => 'Num|Undef');
has ttr     => (is => 'rw', isa => 'Num|Undef');
has space   => (is => 'rw', isa => 'Str|Undef');
has tube    => (is => 'rw', isa => 'Str|Undef');
with 'DR::TarantoolQueue::JSE';


sub tnt {
    my ($self) = @_;

    unless ($self->coro) {
        return $self->{tnt} if $self->{tnt};
        $self->{tnt} = DR::Tarantool::tarantool
            port => $self->port,
            host => $self->host,
            spaces => {}
        ;
    }

    require Coro;
    return $self->{tnt} if $self->{tnt};
    if ($self->{tnt_waiter}) {
        push @{ $self->{tnt_waiter} } => $Coro::current;
        Coro::schedule;
        return $self->{tnt};
    }
    $self->{tnt_waiter} = [];
    $self->{tnt} = DR::Tarantool::coro_tarantool
        port => $self->port,
        host => $self->host,
        spaces => {}
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

    $o->{space} = $self->space unless defined $o->{space};
    croak 'space was not defined' unless defined $o->{space};

    $o->{tube}  = $self->tube unless defined $o->{tube};
    croak 'tube was not defined' unless defined $o->{tube};

    $o->{ttl} ||= $self->ttl || 0;
    $o->{ttr} ||= $self->ttr || 0;
    $o->{delay} ||= 0;
    $o->{pri} ||= 0;

    

    my $tuple = $self->tnt->call_lua(
        "queue.$method" => [
            $o->{space},
            $o->{tube},
            $o->{delay},
            $o->{ttl},
            $o->{ttr},
            $o->{pri},
            $self->jse->encode($o->{data})
        ]
    );

    return DR::TarantoolQueue::Task->tuple($tuple, $o->{space}, $self);
}

=head1 METHODS

=head2 new

    my $q = DR::TarantoolQueue->new(host => 'abc.com', port => 123);

=head3 Options

=over

=item host & port

Host and port where tarantools started.

=item coro (boolean)

If true (default), the queue client will use tarantool's coro client.

=item ttl

Default B<ttl> (time to live) value.

=item ttr

Default B<ttr> (time to release) value.

=item tube

Default queue name.

=item space

Default tarantool's space.

=back

=cut


=head1 Producer methods

=head2 put

    $q->put;
    $q->put(data => { 1 => 2 });
    $q->put(space => 1, tube => 'abc',
            delay => 10, ttl => 3600,
            ttr => 60, pri => 10, data => [ 3, 4, 5 ]);
    $q->put(data => 'string');

Puts task into queue (at end of queue).
Returns L<task|DR::TarantoolQueue::Task> object.

If 'B<space>' and (or) 'B<tube>' aren't defined the method
will try to use them from L<queue|DR::TarantoolQueue/new> object.

=cut

sub put {
    my ($self, %opts) = @_;
    return $self->_producer(put => \%opts);
}

=head2 urgent

The same as L<put|DR::TarantoolQueue/put> but puts task at begin of
queue.

=cut

sub urgent {
    my ($self, %opts) = @_;
    return $self->_producer(urgent => \%opts);
}


=head1 Consumer methods

=head2 take

Takes task for processing.
If timeout is defined and there is no task in queue in the time,
the function returns B<undef>.

    my $task = $q->take;
    my $task = $q->take(timeout => 0.5);
    my $task = $q->take(space => 1, tube => 'requests, timeout => 20);

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

Task was processed (and will be deleted after the call).

    $q->ack(task => $task);
    $task->ack; # the same

=cut

for my $m (qw(ack requeue bury dig unbury delete)) {
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
        return DR::TarantoolQueue::Task->tuple($tuple, $space, $self);
    }
}

=head2 get_meta

Task was processed (and will be deleted after the call).

    my $m = $q->get_meta(task => $task);
    my $m = $q->get_meta(id => $task->id);

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



=head1 COPYRIGHT AND LICENCE

 Copyright (C) 2012 by Dmitry E. Oboukhov <unera@debian.org>
 Copyright (C) 2012 by Roman V. Nikolaev <rshadow@rambler.ru>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

__PACKAGE__->meta->make_immutable();
