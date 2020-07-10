<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use Exception;
use Rabbit\DB\ConnectionInterface;
use Throwable;

/**
 * Class ActiveQuery
 * @package Rabbit\DB\Redisql
 */
class ActiveQuery extends \Rabbit\ActiveRecord\ActiveQuery
{
    use QueryTrait;

    /** @var ActiveRecord */
    public ?ActiveRecord $model = null;

    /**
     * @param ConnectionInterface|null $db
     * @return \Rabbit\DB\Command
     * @throws Throwable
     */
    public function createCommand(ConnectionInterface $db = null): \Rabbit\DB\Command
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;
        $this->model = new $this->modelClass;
        if ($db === null) {
            $db = $modelClass::getDb();
        }

        if ($this->sql === null) {
            [$sql, $params] = $db->getQueryBuilder()->build($this);
        } else {
            $sql = $this->sql;
            $params = $this->params;
        }

        $command = $db->createCommand($sql, $params);
        $this->setCommandCache($command);

        return $command;
    }
}
