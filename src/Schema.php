<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use rabbit\db\Exception;
use rabbit\db\TableSchema;

/**
 * Class Schema
 * @package rabbit\db\clickhouse
 */
class Schema extends \rabbit\db\Schema
{
    /**
     * @param string $name
     * @return TableSchema|null
     */
    protected function loadTableSchema($name)
    {
        return null;
    }

    /**
     * @param string $table
     * @param array $columns
     * @return array|bool|false
     * @throws Exception
     */
    public function insert($table, $columns)
    {
        $command = $this->db->createCommand()->insert($table, $columns);
        if (!$command->execute()) {
            return false;
        }
    }
}
