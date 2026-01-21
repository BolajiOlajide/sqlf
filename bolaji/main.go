package main

import (
	"fmt"

	"github.com/keegancsmith/sqlf"
)

func main() {
	id := 4
	name := "bolaji"
	q := sqlf.Sprintf(`UPDATE changesets
	SET
		batch_change_ids = batch_change_ids - %[1]s::text,
		updated_at = NOW(),
		detached_at = CASE
			WHEN batch_change_ids - %[1]s::text = '{}'::jsonb AND detached_at IS NULL
			THEN NOW()
			ELSE detached_at
		END
	WHERE batch_change_ids ? %[1]s::text AND name = %s`, id, name)
	stmt := q.Query(sqlf.PostgresBindVar)
	args := q.Args()
	fmt.Println(stmt, args)
}
