<?php
namespace EventStore\Broadway;

use Broadway\Domain\DomainEventStreamInterface;
use Broadway\EventStore\EventStoreInterface as BroadwayEventStoreInterface;
use Broadway\EventStore\EventStreamNotFoundException;
use EventStore\EventStoreInterface;
use EventStore\Exception\StreamNotFoundException;
use EventStore\Exception\WrongExpectedVersionException;
use EventStore\WritableEvent;
use EventStore\WritableEventCollection;

class BroadwayEventStore implements BroadwayEventStoreInterface
{
    private $eventStore;

    public function __construct(EventStoreInterface $eventStore)
    {
        $this->eventStore = $eventStore;
    }

    /**
     * @inheritDoc
     */
    public function load($id)
    {
        $iterator = $this
            ->eventStore
            ->forwardStreamFeedIterator($id)
        ;

        try {
            $iterator->rewind();
        } catch (StreamNotFoundException $e) {
            throw new EventStreamNotFoundException($e->getMessage());
        }

        return new DomainEventStream($id, $iterator);
    }

    /**
     * @inheritDoc
     */
    public function append($id, DomainEventStreamInterface $eventStream)
    {
        $events = [];
        $playhead = null;

        foreach ($eventStream as $message) {
            $payload = $message->getPayload();

            if ($playhead === null) {
                $playhead = $message->getPlayhead();
            }

            $events[] = WritableEvent::newInstance(
                get_class($payload),
                array_merge(
                    $payload->serialize(),
                    [
                        'broadway_recorded_on' => $message
                            ->getRecordedOn()
                            ->toString()
                    ]
                ),
                $message
                    ->getMetadata()
                    ->serialize()
            );
        }

        if (empty($events)) {
            return;
        }

        try {
            $this
                ->eventStore
                ->writeToStream(
                    $id,
                    new WritableEventCollection($events),
                    $playhead - 1
                )
            ;
        } catch (WrongExpectedVersionException $e) {
            throw new BroadwayOptimisticLockException($e->getMessage());
        }
    }
}
