<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use rabbit\db\Exception;

/**
 * Class ActiveQuery
 * @package Rabbit\DB\Redisql
 */
class ActiveQuery extends \rabbit\activerecord\ActiveQuery
{
    use QueryTrait;

    /** @var ActiveRecord */
    public $model;

    /**
     * @param null $db
     * @return \rabbit\db\Command
     * @throws \Exception
     */
    public function createCommand($db = null)
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
