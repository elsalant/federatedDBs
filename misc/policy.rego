package dataapi.authz

rule[{"action": {"name": "HashColumn", "description": description, "columns": column_names}}] {
    description := "Hash PII values"
    column_names := [input.resource.metadata.columns[i].name | input.resource.metadata.columns[i].tags.PII]
    count(column_names) > 0
}
