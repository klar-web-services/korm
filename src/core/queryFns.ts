import type { _QueryComparison, _QueryComponent } from "./query";

/**
 * Equality comparison on a JSON path.
 * Use with `QueryBuilder.where(...)`.
 */
export function eq(prop: string, val: any): _QueryComparison {
    return {
        type: "comparison",
        operator: "=",
        property: prop,
        value: val,
    };
}

/**
 * Group components with logical AND.
 */
export function and(...components: _QueryComponent[]): _QueryComponent {
    return {
        type: "group",
        method: "AND",
        components: components,
    };
}

/**
 * Group components with logical OR.
 */
export function or(...components: _QueryComponent[]): _QueryComponent {
    return {
        type: "group",
        method: "OR",
        components: components,
    };
}

/**
 * Negate a component.
 */
export function not(component: _QueryComponent): _QueryComponent {
    return {
        type: "group",
        method: "NOT",
        components: [component],
    };
}

/**
 * Greater-than comparison on a JSON path.
 */
export function gt(prop: string, val: any): _QueryComparison {
    return {
        type: "comparison",
        operator: ">",
        property: prop,
        value: val,
    };
}

/**
 * Greater-than-or-equal comparison on a JSON path.
 */
export function gte(prop: string, val: any): _QueryComparison {
    return {
        type: "comparison",
        operator: ">=",
        property: prop,
        value: val,
    };
}

/**
 * Less-than comparison on a JSON path.
 */
export function lt(prop: string, val: any): _QueryComparison {
    return {
        type: "comparison",
        operator: "<",
        property: prop,
        value: val,
    };
}

/**
 * Less-than-or-equal comparison on a JSON path.
 */
export function lte(prop: string, val: any): _QueryComparison {
    return {
        type: "comparison",
        operator: "<=",
        property: prop,
        value: val,
    };
}

/**
 * SQL LIKE-style comparison on a JSON path.
 * Use `%` and `_` wildcards.
 */
export function like(prop: string, val: any): _QueryComparison {
    return {
        type: "comparison",
        operator: "LIKE",
        property: prop,
        value: val,
    };
}

/**
 * SQL IN-style comparison on a JSON path.
 * Use with `QueryBuilder.where(...)`.
 */
export function inList(prop: string, values: any[]): _QueryComparison {
    return {
        type: "comparison",
        operator: "IN",
        property: prop,
        value: values,
    };
}
