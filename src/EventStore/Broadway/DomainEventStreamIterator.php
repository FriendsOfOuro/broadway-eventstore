<?php
namespace EventStore\Broadway;

use Broadway\Domain\DateTime;
use Broadway\Domain\DomainMessage;
use Broadway\Domain\Metadata;
use EventStore\StreamFeed\StreamFeedIterator;
use Iterator;

final class DomainEventStreamIterator implements Iterator
{
    private $id;

    private $innerIterator;

    public function __construct($id, StreamFeedIterator $innerIterator)
    {
        $this->id = $id;
        $this->innerIterator = $innerIterator;
    }

    public function valid()
    {
        return $this->innerIterator->valid();
    }

    public function next()
    {
        $this->innerIterator->next();
    }

    public function key()
    {
        list($key,) = explode('@', $this->innerIterator->key());

        return (int) $key;
    }

    public function rewind()
    {
        return $this->innerIterator->rewind();
    }

    public function current()
    {
        $event = $this->innerIterator->current()->getEvent();

        $data = $event->getData();
        $recordedOn = DateTime::fromString($data['broadway_recorded_on']);
        unset($data['broadway_recorded_on']);

        return new DomainMessage(
            $this->id,
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
}
