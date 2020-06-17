<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use rabbit\activerecord\BaseActiveRecord;
use rabbit\core\ObjectFactory;
use rabbit\db\ConnectionInterface;

/**
 * Class ActiveRecord
 * @package Rabbit\DB\Redisql
 */
class ActiveRecord extends \rabbit\activerecord\ActiveRecord
{
    /**
     * @return ConnectionInterface
     * @throws Exception
     */
    public static function getDb(): ConnectionInterface
    {
        return getDI('redisql')->getConnection();
    }

    /**
     * @return string[]
     */
    public static function primaryKey()
    {
        return ['id'];
    }

    /**
     * @return mixed|\rabbit\activerecord\ActiveQuery
     * @throws DependencyException
     * @throws NotFoundException
     */
    public static function find()
    {
        return ObjectFactory::createObject(ActiveQuery::class, ['modelClass' => get_called_class()], false);
    }

    /**
     * @return mixed|\rabbit\db\TableSchema|null
     * @throws Exception
     */
    public static function getTableSchema()
    {
        return getDI(TableSchema::class);
    }

    /**
     * @param BaseActiveRecord $record
     * @param array $row
     */
    public static function populateRecord($record, $row)
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
     * @param null $attributes
     * @return bool
     * @throws Exception
     */
    protected function insertInternal($attributes = null)
    {
        $values = $this->getDirtyAttributes($attributes);
        if (($primaryKeys = static::getDb()->schema->insert(static::tableName(), $values)) === false) {
            return false;
        }
        $this->setOldAttributes($values);
        return true;
    }

    /**
     * @param null $attributes
     * @return false|int
     * @throws \rabbit\db\Exception
     */
    protected function updateInternal($attributes = null)
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