# S-expressions

S-expressions is the way Walkable allow you to write arbitrary SQL
expressions in your
[paredit](https://github.com/clojure-emacs/cider)/[parinfer](https://github.com/shaunlebron/parinfer)-[powered](https://github.com/tpope/vim-fireplace)
[editors](https://cursive-ide.com/) without compromising security.

## A tour of Walkable S-expressions

> Note about SQL examples:

  - S-expressions can end up as SQL strings in either `SELECT`
  statements or `WHERE` conditions. For demonstrating purpose, the
  strings are wrapped in `SELECT ... as q` so the SQL outputs are
  executable, except ones with tables and columns.

  - SQL output may differ when you `require` different implementations
    (ie `(require 'walkable.sql-query-builder.impl.postgres)` vs
    `(require 'walkable.sql-query-builder.impl.sqlite)`).

### Primitive types

```clj
;; expression
123
;; sql output
(jdbc/query your-db ["SELECT 123 AS q"])
;; => [{:q 123}]

;; expression
nil
;; sql output
(jdbc/query your-db ["SELECT NULL AS q"])
;; => [{:q nil}]

;; expression
"hello world"
;; sql output
(jdbc/query your-db ["SELECT ? AS q" "hello world"])
;; => {:q "hello world"}

;; expression
"hello\"; DROP TABLE users"
;; sql output
(jdbc/query your-db ["SELECT ? AS q" "hello\"; DROP TABLE users"])
;; => {:q "hello\"; DROP TABLE users"}
```

### Columns

> Note: The examples just use backticks as quote marks. Depending on
  your schema configuration, Walkable will emit SQL strings using
  whatever quote marks you specified.

```clj
;; expression
:my-table/a-column
;; sql output
(jdbc/query your-db ["SELECT `my_table`.`a_column` AS `my-table/a-column` FROM `my_table`"])
;; => [{:my-table/a-column 42}, ...other records...]
```

### Comparison

Walkable comes with some comparison operators: `:=`, `:<`, `:>`,
`:<=`, `:>=`. They will result in SQL operators with the same name,
but also handle multiple arity mimicking their Clojure equivalents.

```clj
;; expression
[:= 1 2]
;; sql output
(jdbc/query your-db ["SELECT (1 = 2) AS q"])
;; => [{:q false}]

;; expression
[:<= 1 2]
;; sql output
(jdbc/query your-db ["SELECT (1 <= 2) AS q"])
;; => [{:q true}]

;; expression
[:< 1 2 3 1]
;; sql output
(jdbc/query your-db ["SELECT ((1 < 2) AND (2 < 3) AND (3 < 1)) AS q"])
;; => [{:q false}]

;; expression
[:= 0]
;; sql output
(jdbc/query your-db ["SELECT true AS q"])
;; => [{:q true}]

;; expression
[:>= 1000]
;; sql output
(jdbc/query your-db ["SELECT true AS q"])
;; => [{:q true}]
```

String comparison operators: `=`, `like`, `match`, `glob`:

```clj
;; expression
[:= "hello" "hi"]
;; sql output
(jdbc/query your-db ["SELECT (? = ?) AS q" "hello" "hi"])
;; => [{:q false}]

;; expression
[:like "abcd" "abc%"]
;; sql output
(jdbc/query your-db ["SELECT (? LIKE ?) AS q" "abcd" "abc%"])
;; => [{:q true}]
```

Use them on some columns, too:

```clj
;; expression
[:= :my-table/its-column "hi"]
;; sql output
(jdbc/query your-db ["SELECT (`my_table`.`its_column` = ?) AS q FROM `my_table`" "hi"])
;; => [{:q true}]
```

### Math

Basic math operators work just like their Clojure equivalents: `:+`,
`:-`, `:*`, `:/`:

```clj
;; expression
[:+ 1 2 4 8]
;; sql output
(jdbc/query your-db ["SELECT (1 + 2 + 4 + 8) AS q"])
;; => [{:q 15}]
```

Feel free to mix them

```clj
;; expression
[:+ [:*] [:* 2 4 7] [:/ 0.25]]
;; sql output
(jdbc/query your-db ["SELECT (1 + (2 * 4 * 7) + (1/0.25)) AS q"])
;; => [{:q 61.0}]
```

(`:*` with no argument result in `1`)

### String manipulation

```clj
;; expression
[:str "hello " nil "world" 123]
;; sql output
(jdbc/query your-db ["SELECT (CONCAT(?, NULL, ?, 123) AS q" "hello " "world"])
;; => [{:q "hello world123"}]

;; expression
[:subs "hello world"]
;; sql output
(jdbc/query your-db ["SELECT (CONCAT(?, NULL, ?, 123) AS q" "hello " "world"])
;; => [{:q "hello world123"}]

;; expression
[:str "hello " nil "world" 123]
;; sql output
(jdbc/query your-db ["SELECT (CONCAT(?, NULL, ?, 123) AS q" "hello " "world"])
;; => [{:q "hello world123"}]

```

### Conversion between types

Use the `:cast` operator:

```clj
;; expression
[:cast "2" :integer]
;; sql output
(jdbc/query your-db ["SELECT CAST(? as INTEGER) AS q" "2"])
;; => [{:q 2}]

;; expression
[:cast 3 :text]
;; sql output
(jdbc/query your-db ["SELECT CAST(3 as TEXT) AS q"])
;; => [{:q "3"}]
```

### Logic constructs

`:and` and `:or` accept many arguments like in Clojure:

```clj
;; expression
[:and true true false]
;; sql output
(jdbc/query your-db ["SELECT (true AND true AND false) AS q"])
;; => [{:q false}]

;; expression
[:and]
;; sql output
(jdbc/query your-db ["SELECT (true) AS q"])
;; => [{:q true}]

;; expression
[:or]
;; sql output
(jdbc/query your-db ["SELECT (NULL) AS q"])
;; => [{:q nil}]
```

`:not` accepts exactly one argument:

```
;; expression
[:not true]
;; sql output
(jdbc/query your-db ["SELECT (NOT true) AS q"])
;; => [{:q false}]
```

Party time! Mix them as you wish:

```clj
;; expression
[:and [:= 4 [:* 2 2]] [:not [:> 1 2]] [:or [:= 2 3] [:= 4 4]]]
;; sql output
(jdbc/query your-db ["SELECT (((4)=((2)*(2))) AND (NOT ((1)>(2))) AND (((2)=(3)) OR ((4)=(4)))) AS q"])
;; => [{:q true}]
```

Please note that Walkable S-expressions are translated directly to SQL
equivalent. Your DBMS may throw an exception if you ask for this:

```clj
;; expression
[:or 2 true]
;; sql output
(jdbc/query your-db ["SELECT (2 OR true) AS q"])
;; =>ERROR:  argument of OR must be type boolean, not type integer
```

Don't be surprised if you see `[:not nil]` is ... `nil`!

```
;; expression
[:not nil]
;; sql output
(jdbc/query your-db ["SELECT (NOT NULL) AS q"])
;; => [{:q nil}]
```

### Other constructs

`:when`, `:if`, `:case` and `:cond` look like in Clojure...

```clj
;; expression
[:when true "yay"] ;; or [:if true "yay"]
;; sql output
(jdbc/query your-db ["SELECT (CASE WHEN ( true ) THEN ( ? ) END) AS q" "yay"])
;; => [{:q "yay"}]

;; expression
[:if [:= 1 2] "yep" "nope"]
;; sql output
(jdbc/query your-db ["SELECT (CASE WHEN ((1)=(2)) THEN ( ? ) ELSE ( ? ) END) AS q" "yay" "nope"])
;; => [{:q "nope"}]

;; expression
[:case [:+ 0 1] 2 3]
;; sql output
(jdbc/query your-db ["SELECT (CASE (0+1) WHEN (2) THEN (3) END) AS q"])
;; => [{:q nil}]

;; expression
[:case [:+ 0 1] 2 3 4]
;; sql output
(jdbc/query your-db ["SELECT (CASE (0+1) WHEN (2) THEN (3) ELSE (4) END) AS q"])
;; => [{:q 4}]

;; expression
[:cond [:= 0 1] "wrong" [:< 2 3] "right"]
;; sql output
(jdbc/query your-db ["SELECT  (CASE WHEN ((0)=(1)) THEN ( ? ) WHEN ((2)<(3)) THEN ( ? ) END) AS q" "wrong" "right"])
;; => [{:q "right"}]
```

...except the fact that you must supply real booleans to them, not
just some truthy values.

```clj
;; expression
[:cond
 [:= 0 1]
 "wrong"

 [:> 2 3]
 "wrong again"

 true ;; <= must be literally `true`, not `:default` or something else
 "default"]
;; sql output
(jdbc/query your-db ["SELECT  (CASE WHEN ((0)=(1)) THEN ( ? ) WHEN ((2)>(3)) THEN ( ? ) WHEN ( true ) THEN ( ? ) END) AS q" "wrong" "wrong again" "default"])
;; => [{:q "default"}]
```

### Pseudo columns

In your schema you can define so-called pseudo columns that look just
like normal columns from client-side view:

```clj
;; schema
;; :person/yob is a real column
{:pseudo-columns {:person/age [:- 2018 :person/yob]}}
```

You can't tell the difference from client-side:

```clj
;; query for a real column
[{[:person/by-id 9]
  [:person/yob]}]
;; query for a pseudo column
[{[:person/by-id 9]
  [:person/age]}]

;; filter with a real column
[{(:people/all {:filters [:= 1988 :person/yob]})
  [:person/name]}]
;; filter with a pseudo column
[{(:people/all {:filters [:= 30 :person/age]})
  [:person/name]}]
```

Behind the scenes, Walkable will expand the pseudo columns to whatever
they are defined. You can also use pseudo columns in other pseudo
columns' definition, but be careful as Walkable **won't check circular
dependencies** for you.

Please note you can only use true columns from the same table in the
definition of pseudo columns. For instance, the following doesn't make
sense:

```clj
;; schema
{:pseudo-columns {:person/age [:- 2018 :pet/yob]}}
```

Your RDMS will throw an exception in that case anyway.

## Define your own operators

There are some convenient marcros to help you "import" SQL
functions/operators:
`walkable.sql-query-builder.expressions/import-functions` and
`walkable.sql-query-builder.expressions/import-infix-operators`.

More complex operators may require implementing multimethod
`walkable.sql-query-builder.expressions/process-operator` or even a
harder one
`walkable.sql-query-builder.expressions/process-unsafe-expression`.

Todo: more docs.

## Bonus: JSON in Postgresql

The following expressions work in Postgresql:

```clj
;; expression
[:= 1
 [:cast [:get-as-text [:jsonb {:a 1}] "a"] :integer]]
;; sql output
(jdbc/query your-db ["SELECT ((1)=(CAST ((?::jsonb)->>( ? ) AS INTEGER))) AS q" "{\"a\" :1}" "a"])
;; => [{:q true}]

;; expression
[:or [:= 2 [:array-length [:array 1 2 3 4] 1]]
 [:contains [:jsonb {:a 1 :b 2}]
  [:jsonb {:a 1}]]
 [:jsonb-exists [:jsonb {:a 1 :b 2}]
  "a"]]
;; sql output
(jdbc/query your-db ["SELECT (((2)=(array_length (ARRAY[1, 2, 3, 4], 1)))
OR ((?::jsonb)@>(?::jsonb))
OR (jsonb_exists (?::jsonb,  ? )))
AS q"
    "{\"a\":1,\"b\":2}" "{\"a\":1}" "{\"a\":1,\"b\":2}" "a"])
;; => [{:q true}]
```