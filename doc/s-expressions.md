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

Todo: List of available cast types.

### Logic constructs

:and, :or, not

### Other constructs

:when, :if, :case, :cond

### Pseudo columns

TBD