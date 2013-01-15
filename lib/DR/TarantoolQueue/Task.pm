package DR::TarantoolQueue::Task;
use utf8;
use strict;
use warnings;
use Mouse;
use JSON::XS ();
use Carp;

has space   => (is => 'ro', isa => 'Str', required => 1);
has tube    => (is => 'ro', isa => 'Str', required => 1);
has id      => (is => 'ro', isa => 'Str', required => 1);
has rawdata => (is => 'ro', isa => 'Str', required => 1);
has data    => (
    is          => 'ro',
    isa         => 'HashRef|ArrayRef|Undef',
    lazy        => 1,
    builder     => '_build_data'
);

has queue   => (is => 'ro', isa => 'Object|Undef', weak_ref => 1);

with 'DR::TarantoolQueue::JSE';


sub _build_data {
    my ($self) = @_;
    $self->jse->decode( $self->rawdata );
}


sub ack {
    my ($self) = @_;
    croak "Can't find queue for task" unless $self->queue;
    return $self->queue->ack(task => $self);
}

__PACKAGE__->meta->make_immutable();
