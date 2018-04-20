# S-expressions

S-expressions is the way Walkable allow you to write arbitrary SQL
expressions in your
[paredit](https://github.com/clojure-emacs/cider)/[parinfer](https://github.com/shaunlebron/parinfer)-[powered](https://github.com/tpope/vim-fireplace)
[editors](https://cursive-ide.com/) without compromising security.

## A tour of Walkable S-expressions

> Note about SQL examples: S-expressions can be used in both SQL
  `SELECT` statements and `WHERE` conditions. For demonstrating
  purpose, the `SELECT` parts are added so the SQL output is
  executable.

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
:cast

### Logic constructs

:and, :or, not

### Other constructs

:when, :if, :case, :cond

### Pseudo columns

TBD