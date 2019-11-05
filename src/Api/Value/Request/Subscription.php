<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Api\Value\Request;

use JMS\Serializer\Annotation as Serializer;

final class Subscription
{
    /**
     * @Serializer\Type("iterable<string>")
     * @Serializer\Accessor(getter="getTopics")
     * @Serializer\SkipWhenEmpty()
     * @var iterable<string>|null
     */
    private $topics;

    /**
     * @Serializer\Type("string")
     * @Serializer\SerializedName("topic_pattern")
     * @Serializer\Accessor(getter="getPattern")
     * @Serializer\SkipWhenEmpty()
     * @var string|null
     */
    private $pattern;

    /**
     * @param iterable<string>|null $topics
     */
    private function __construct(?iterable $topics, ?string $pattern)
    {
        $this->topics = $topics;
        $this->pattern = $pattern;
    }

    public static function topic(string $topic) : self
    {
        return new self([$topic], null);
    }

    /**
     * @param iterable<string> $topics
     */
    public static function topics(iterable $topics) : self
    {
        return new self($topics, null);
    }

    public static function pattern(string $pattern) : self
    {
        return new self(null, $pattern);
    }

    /**
     * @return string[]|null
     */
    public function getTopics() : ?iterable
    {
        return $this->topics;
    }

    public function getPattern() : ?string
    {
        return $this->pattern;
    }
}
