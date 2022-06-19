# mv-elastic

The elastic search module for Meveo is designed to add the ElasticSearch storage to Meveo available storages.

- [mv-elastic](#mv-elastic)
  - [Requirements](#requirements)
  - [Known limitations](#known-limitations)
  - [Configuration](#configuration)
  - [Implementation details](#implementation-details)
  - [Auto-completion](#auto-completion)

## Requirements

- Meveo >= 7.1
- ElasticSearch : 8.2 (not tested for other versions)

## Known limitations

- No transaction mangagements

## Configuration

- Go to Configuration > Storages > Repositories
- Select one repository
- Fill the informations in the `Elastic Search` tab.

A next evolution will be to store the password encrypted.

## Implementation details

When a CET is created, a corresponding index, with lower-cased code, is created in the ElasticSearch instance.

When a CFT is created, the mapping of the index is updated with below details. All fields name are lower-cased as well.

CFT fields type mapping : 
- `LONG_TEXT` => `text` used to make full text search quries
- `STRING` => `search_as_you_type` used to make autocomplete or wildcard queries

GUI and CrossStorage request :
- Long text fields filters are converted to full-text query
- String text fields filters are converted to wildcard query

When a CET is deleted, the index is deleted if the "remove data" option was used.

## Auto-completion

The auto-completion endpoint is available at 
```shell
POST /meveo/rest/autocomplete/{entity}/{field}
{
    "query": {query},
    (optional) "repository": {repository}
}
```