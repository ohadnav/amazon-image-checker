from sp_api.api import CatalogItems as Catalog, CatalogItemsVersion


def main():
    res = Catalog(version=CatalogItemsVersion.V_2020_12_01).get_catalog_item('B07Z95MG3S')
    print(res)


if __name__ == '__main__':
    main()
