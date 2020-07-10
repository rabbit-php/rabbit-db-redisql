<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use DI\DependencyException;
use DI\NotFoundException;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\DB\Exception;
use Rabbit\DB\TableSchema;
use Throwable;

/**
 * Class Schema
 * @package Rabbit\DB\Redisql
 */
class Schema extends \Rabbit\DB\Schema
{
    /** @var string */
    protected string $builderClass = QueryBuilder::class;

    /**
     * @param string $name
     * @return TableSchema|null
     */
    protected function loadTableSchema(string $name): ?TableSchema
    {
        return null;
    }

    /**
     * @param string $table
     * @param array $columns
     * @return array|bool|false
     * @throws DependencyException
     * @throws NotFoundException
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Exception
     * @throws Throwable
     */
    public function insert(string $table, array $columns): ?array
    {
        $command = $this->db->createCommand()->insert($table, $columns);
        if (!$command->execute()) {
            return null;
        }
        return $columns;
    }
}
