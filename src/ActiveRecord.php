<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\ActiveRecord\ActiveQueryInterface;
use Rabbit\ActiveRecord\BaseActiveRecord;
use Rabbit\DB\Exception;
use Rabbit\Pool\ConnectionInterface;
use ReflectionException;
use Throwable;

/**
 * Class ActiveRecord
 * @package Rabbit\DB\Redisql
 */
class ActiveRecord extends \rabbit\activerecord\ActiveRecord
{
    /**
     * @return ConnectionInterface
     * @throws Throwable
     */
    public static function getDb(): ConnectionInterface
    {
        return getDI('redisql')->get();
    }

    /**
     * @return array|string[]
     */
    public static function primaryKey(): array
    {
        return ['id'];
    }

    /**
     * @return mixed|\rabbit\activerecord\ActiveQuery
     * @throws DependencyException
     * @throws NotFoundException
     */
    public static function find(): ActiveQueryInterface
    {
        return create(ActiveQuery::class, ['modelClass' => get_called_class()], false);
    }

    /**
     * @return \Rabbit\DB\TableSchema
     * @throws Throwable
     */
    public static function getTableSchema(): \Rabbit\DB\TableSchema
    {
        return getDI(TableSchema::class);
    }

    /**
     * @param BaseActiveRecord $record
     * @param array $row
     * @throws ReflectionException
     */
    public static function populateRecord(BaseActiveRecord $record, array $row): void
    {
        $columns = array_flip($record->attributes());
        foreach ($row as $name => $value) {
            if (isset($columns[$name])) {
                $record->_attributes[$name] = $value;
            } elseif ($record->canSetProperty($name)) {
                $record->$name = $value;
            }
        }
        $record->_oldAttributes = $record->_attributes;
        $record->_related = [];
        $record->_relationsDependencies = [];
    }

    /**
     * @param array|null $attributes
     * @return bool
     * @throws ReflectionException
     * @throws Throwable
     */
    protected function insertInternal(array $attributes = null): bool
    {
        $values = $this->getDirtyAttributes($attributes);
        if (($primaryKeys = static::getDb()->schema->insert(static::tableName(), $values)) === false) {
            return false;
        }
        $this->setOldAttributes($values);
        return true;
    }

    /**
     * @param array|null $attributes
     * @return int
     * @throws ReflectionException
     * @throws Throwable
     * @throws Exception
     */
    protected function updateInternal(array $attributes = null): int
    {
        $values = $this->getDirtyAttributes($attributes);
        if (empty($values)) {
            return 0;
        }
        $condition = $this->getOldPrimaryKey(true);
        $rows = static::updateAll($values, $condition);
        $changedAttributes = [];
        foreach ($values as $name => $value) {
            $changedAttributes[$name] = isset($this->_oldAttributes[$name]) ? $this->_oldAttributes[$name] : null;
            $this->_oldAttributes[$name] = $value;
        }
        return $rows;
    }
}