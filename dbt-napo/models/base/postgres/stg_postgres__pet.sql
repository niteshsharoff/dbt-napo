select pk, fields.* from {{ source("postgres", "pet") }}
