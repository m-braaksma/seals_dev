# SEALS

See the [SEALS documentation](https://justinandrewjohnson.com/earth_economy_devstack/seals_overview.html) for full details.

## Developer notes

To manually (wihtout using github actions) push a new release to Pypi, run the following command:

```bash
python -m build
python -m twine upload dist/*
```