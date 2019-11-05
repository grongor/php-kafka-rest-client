<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Request;

use JMS\Serializer\Annotation as Serializer;
use function base64_encode;

final class Message
{
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

    public function __construct(string $content, ?string $key = null)
    {
        $this->content = base64_encode($content);
        $this->key = $key === null ? null : base64_encode($key);
    }
}
