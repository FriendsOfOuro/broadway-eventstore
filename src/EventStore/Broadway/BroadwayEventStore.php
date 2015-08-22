<?php
namespace EventStore\Broadway;

use Broadway\Domain\DateTime;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainEventStreamInterface;
use Broadway\Domain\DomainMessage;
use Broadway\Domain\Metadata;
use Broadway\EventStore\EventStoreInterface as BroadwayEventStoreInterface;
use Broadway\EventStore\EventStreamNotFoundException;
use EventStore\EventStoreInterface;
use EventStore\Exception\StreamNotFoundException;
use EventStore\Exception\WrongExpectedVersionException;
use EventStore\StreamFeed\Entry;
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

        $messages = [];

        $i = 0;

        try {
            foreach ($iterator as $entryWithEvent) {
                $event = $entryWithEvent->getEvent();

                $data = $event->getData();
                $recordedOn = DateTime::fromString($data['broadway_recorded_on']);
                unset($data['broadway_recorded_on']);

                $messages[] = new DomainMessage(
                    $id,
                    $event->getVersion(),
                    new Metadata($event->getMetadata() ?: []),
                    call_user_func(
                        [
                            $event->getType(),
                            'deserialize'
                        ],
                        $data
                    ),
                    $recordedOn
                );
            }
        } catch (StreamNotFoundException $e) {
            throw new EventStreamNotFoundException($e->getMessage());
        }

        return new DomainEventStream($messages);
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

    /**
     * @param  Entry[] $entries
     * @return Entry[]
     */
    private function sortEntries(array $entries)
    {
        usort(
            $entries,
            function ($a, $b) {
                return $this->getVersion($a) - $this->getVersion($b);
            }
        );

        return $entries;
    }

    private function getVersion(Entry $entry)
    {
        $parts = explode('/', $entry->getEventUrl());

        return (int) array_pop($parts);
    }
}
