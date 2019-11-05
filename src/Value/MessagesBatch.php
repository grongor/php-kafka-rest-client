<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Value;

use Grongor\KafkaRest\Api\Value\Response\Message;
use function count;

final class MessagesBatch
{
    /** @var array<Message> */
    private $messages = [];

    public function add(Message $message) : void
    {
        $this->messages[] = $message;
    }

    /**
     * @return Message[]
     */
    public function getMessages() : array
    {
        return $this->messages;
    }

    public function count() : int
    {
        return count($this->messages);
    }

    public function isEmpty() : bool
    {
        return count($this->messages) === 0;
    }
}
