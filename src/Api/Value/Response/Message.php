<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Response;

use JMS\Serializer\Annotation as Serializer;
use function Safe\base64_decode;

final class Message
{
    /**
     * @Serializer\Type("string")
     * @var string
     */
    public $topic;

    /**
     * @Serializer\Type("int")
     * @var int
     */
    public $partition;

    /**
     * @Serializer\Type("string")
     * @var string|null
     */
    public $key;

    /**
     * @Serializer\Type("string")
     * @Serializer\SerializedName("value")
     * @var string
     */
    public $content;

    /**
     * @Serializer\Type("int")
     * @var int
     */
    public $offset;

    /**
     * @Serializer\Type("int")
     * @var int
     */
    public $timestamp;

    /**
     * @internal
     *
     * @Serializer\PostDeserialize()
     */
    public function deserialize() : void
    {
        $this->content = base64_decode($this->content, true);

        if ($this->key === null) {
            return;
        }

        $this->key = base64_decode($this->key, true);
    }
}
