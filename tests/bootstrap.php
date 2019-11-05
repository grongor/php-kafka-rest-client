<?php

declare(strict_types=1);

namespace Grongor\KafkaRest\Tests;

use Doctrine\Common\Annotations\AnnotationRegistry;
use function date_default_timezone_set;
use function error_reporting;
use const E_ALL;

require __DIR__ . '/../vendor/autoload.php';
AnnotationRegistry::registerLoader('class_exists');

error_reporting(E_ALL);
date_default_timezone_set('UTC');
