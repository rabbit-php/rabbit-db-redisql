<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use Exception;

/**
 * Class Query
 * @package Rabbit\DB\Redisql
 */
class Query extends \rabbit\db\Query
{
    use QueryTrait;

    /**
     * @param null $db
     * @return \rabbit\db\Command
     * @throws Exception
     */
    public function createCommand($db = null)
    {
        if ($db === null) {
            $db = getDI('redisql')->getConnection();
        }
        [$sql, $params] = $db->getQueryBuilder()->build($this);

        $command = $db->createCommand($sql, $params);
        $this->setCommandCache($command);

        return $command;
    }
}