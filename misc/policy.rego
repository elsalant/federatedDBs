package dataapi.authz

rule[{"action": {"name": "HashColumn", "description": description, "columns": column_names}, "intent":intent}] {
    description := "Hash PII values"
    columns := [input.resource.metadata.columns[i].name | input.resource.metadata.columns[i].tags.PII]
    intent := "research"
    count(columns) > 0
}
