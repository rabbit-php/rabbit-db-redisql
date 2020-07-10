<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use Rabbit\DB\ConnectionInterface;
use Throwable;

/**
 * Class Query
 * @package Rabbit\DB\Redisql
 */
class Query extends \Rabbit\DB\Query
{
    use QueryTrait;

    /**
     * @param ConnectionInterface|null $db
     * @return \Rabbit\DB\Command
     * @throws Throwable
     */
    public function createCommand(ConnectionInterface $db = null): \Rabbit\DB\Command
    {
        if ($db === null) {
            $db = getDI('redisql')->get();
        }
        [$sql, $params] = $db->getQueryBuilder()->build($this);

        $command = $db->createCommand($sql, $params);
        $this->setCommandCache($command);

        return $command;
    }
}