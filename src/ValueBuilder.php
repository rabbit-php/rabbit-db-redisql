<?php
declare(strict_types=1);

namespace Rabbit\DB\Redisql;

use rabbit\db\ExpressionBuilderInterface;
use rabbit\db\ExpressionInterface;

/**
 * Class ValueBuilder
 * @package Rabbit\DB\Redisql
 */
class ValueBuilder implements ExpressionBuilderInterface
{
    /**
     * {@inheritdoc}
     */
    public function build(ExpressionInterface $expression, array &$params = [])
    {
        $placeholder = '?' . (count($params) + 1);
        $params[$placeholder] = $expression;

        return $placeholder;
    }
}