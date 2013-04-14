use utf8;
use strict;
use warnings;
package DR::TarantoolQueue::Worker;
use Carp;
use Mouse;
use Coro;

=head1 NAME

DR::TarantoolQueue::Worker - template for workers

=head1 SYNOPSIS

    my $worker = DR::TarantoolQueue::Worker->new(
        count       => 10,      # defaults 1
        queue       => $queue
    );

    sub process {
        my ($task) = @_;


        ... do something with task


    }

    $worker->run(\&process)

=head1 DESCRIPTION

=over

=item *

Process function can throw exception. The task will be buried (if process
function didn't change task status yet.

=item *

If process function didn't change task status (didn't call B<ack>, or
L<DR::TarantoolQueue::Task/release>) worker calls
L<DR::TarantoolQueue::Task/ack>.

=item *

L<run> method catches B<SIGTERM> and B<SIGINT> and waits for all process
functions are done and then do return.

=item *

Worker uses default B<tube> and B<space> in queue. So You have to define
them in Your queue or here.

=back

=head1 ATTRIBUTES

=cut

=head2 count

Count of process functions that can do something at the same time.
Default value is B<1>. The attribute means something if Your B<process>
function uses L<Coro> and Your queue uses L<Coro>, too.

=cut

has count       => isa => 'Num',                is => 'ro', default => 1;


=head2 queue

Ref to Your queue.

=cut

has queue       => isa => 'DR::TarantoolQueue', is => 'ro', required => 1;

=head2 space & tube

Space and tube for processing queue.

=cut

has space           => isa => 'Str|Undef', is => 'ro';
has tube            => isa => 'Str|Undef', is => 'ro';

=head1 PRIVATE ATTRIBUTES

=head2 timeout

timeout for queue.take

=cut

has timeout         => isa => 'Num', is => 'ro', default => 2;

=head2 is_run

B<True> means that workers are run

=cut

has is_run          => isa => 'Bool', is => 'rw', default => 0;

=head2 is_stopping

B<True> means that workers are stopping (by B<SIGTERM>/B<SIGINT>/L<stop>)

=cut

has is_stopping     => isa => 'Bool', is => 'rw', default => 0;


has stop_waiters    => isa => 'ArrayRef', is => 'ro', default => sub {[]};


=head1 METHODS

=head2 run(CODEREF[, CODEREF])

Run workers. Two arguments:

=over

=item process function

=item debug function

The function can be used to show internal debug messages.

=back

=cut

sub run {
    my ($self, $cb, $debugf) = @_;
    croak 'process subroutine is not CODEREF' unless 'CODE' eq ref $cb;
    $debugf //= sub {  };
    croak 'debugf subroutine is not CODEREF' unless 'CODE' eq ref $debugf;

    croak 'worker is already run' if $self->is_run;

    local $SIG{TERM} = sub {
        $debugf->('SIGTERM was received, stopping...');
        $self->is_stopping( 1 )
    };
    local $SIG{INT}  = sub {
        $debugf->('SIGINT was received, stopping...');
        $self->is_stopping( 1 )
    };

    my @f;
    
    $self->is_run( 1 );
    $self->is_stopping( 0 );

    for (1 .. $self->count) {
        push @f => async {
            while($self->is_run and !$self->is_stopping) {
                my $task = $self->queue->take(
                    defined($self->space) ? (space => $self->space) : (),
                    defined($self->tube)  ? (tube  => $self->tube)  : (),
                    timeout => $self->timeout,
                );
                next unless $task;

                eval {
                    $cb->( $task );
                };

                if ($@) {
                    $debugf->('Worker was died (%s)', $@);
                    if ($task->status eq 'taken') {
                        $debugf->('task %s will be buried', $task->id);
                        eval { $task->bury };
                        if ($@) {
                            $debugf->("Can't bury task %s: %s", $task->id, $@);
                        }
                    }
                    next;
                }
                if ($task->status eq 'taken') {
                    $debugf->('task %s was processed, will be ack', $task->id);
                    eval { $task->ack };
                    if ($@) {
                        $debugf->("Can't ack task %s: %s", $task->id, $@);
                    }
                    next;
                }
            }
        }
    }

    $_->join for @f;
    $self->is_run( 0 );
    $self->is_stopping( 0 );
    while(@{ $self->stop_waiters }) {
        my $w = shift @{ $self->stop_waiters };
        $w->ready;
    }
    return $self->count;
}


=head2 stop

Stop worker cycle

=cut

sub stop {
    my ($self) = @_;
    return 0 unless $self->is_run;
    $self->is_stopping( 1 );
    push @{ $self->stop_waiters } => $Coro::current;
    Coro::schedule;
    return $self->is_run;
}

__PACKAGE__->meta->make_immutable();

