# Saídas das consultas — banco `ecommerce_uk`

## Documentos por coleção

- `pedidos`: 23,195
- `clientes`: 4,362
- `produtos`: 3,817

## Exemplo de documento da coleção `pedidos`

```json
{
  "_id": "536365",
  "data_hora": "2010-12-01T08:26:00",
  "cliente_id": 17850,
  "pais": "United Kingdom",
  "cancelado": false,
  "itens": [
    {
      "codigo_produto": "21730",
      "descricao": "GLASS STAR FROSTED T-LIGHT HOLDER",
      "quantidade": 6,
      "preco_unitario_gbp": 4.25,
      "valor_item_gbp": 25.5
    },
    {
      "codigo_produto": "22752",
      "descricao": "SET 7 BABUSHKA NESTING BOXES",
      "quantidade": 2,
      "preco_unitario_gbp": 7.65,
      "valor_item_gbp": 15.3
    }
  ],
  "total_itens": 7,
  "valor_total_gbp": 139.12
}
// ... (itens truncados para exibição)
```

## Receita por mes

```
    mes  pedidos  receita_gbp
2010-12     1550    775714.95
2011-01     1081    670439.46
2011-02     1093    507866.54
2011-03     1440    689841.84
2011-04     1235    515469.66
2011-05     1668    740036.33
2011-06     1525    737683.99
2011-07     1452    688252.67
2011-08     1339    724308.16
2011-09     1818   1028345.38
2011-10     2005   1103330.92
2011-11     2751   1452115.98
2011-12      816    614499.25
```

## Top paises receita

```
          pais  pedidos  receita_gbp
United Kingdom    17901   8725129.48
   Netherlands       93    283889.34
       Ireland      282    270850.86
       Germany      443    205381.15
        France      382    184493.00
     Australia       56    138103.81
         Spain       88     55706.56
   Switzerland       50     53065.60
         Japan       19     37416.37
       Belgium       98     36927.34
```

## Top produtos dez2010

```
codigo_produto                          descricao  quantidade  receita_gbp
         84077  WORLD WAR 2 GLIDERS ASSTD DESIGNS        5195      1174.13
         21212    PACK OF 72 RETROSPOT CAKE CASES        4106      2681.58
        85123A WHITE HANGING HEART T-LIGHT HOLDER        3870     11234.22
         22834        HAND WARMER BABUSHKA DESIGN        3476      4655.28
         22197               SMALL POPCORN HOLDER        2737      2629.94
         22492             MINI PAINT SET VINTAGE        2710      1616.80
         22616          PACK OF 12 LONDON TISSUES        2694       750.10
         22693 GROW A FLYTRAP OR SUNFLOWER IN TIN        2617      2509.01
         22086     PAPER CHAIN KIT 50'S CHRISTMAS        2454      9208.10
         84879      ASSORTED COLOUR BIRD ORNAMENT        2363      3972.35
```

## Top produtos dez2011

```
codigo_produto                         descricao  quantidade  receita_gbp
         23843       PAPER CRAFT , LITTLE BIRDIE       80995    168469.60
         22197                    POPCORN HOLDER        6217      5423.80
         23084                RABBIT NIGHT LIGHT        4304      9628.46
         23582       VINTAGE DOILY JUMBO BAG RED        2101      3852.28
         22086    PAPER CHAIN KIT 50'S CHRISTMAS        2088      6867.76
         21137          BLACK RECORD COVER FRAME        1574      5582.43
         20668   DISCO BALL CHRISTMAS DECORATION        1474       175.20
         22413    METAL SIGN TAKE IT OR LEAVE IT        1450      3996.70
         84077 WORLD WAR 2 GLIDERS ASSTD DESIGNS        1367       379.17
         84879     ASSORTED COLOUR BIRD ORNAMENT        1330      2162.74
```

## Clientes vip

```
 cliente_id           pais  total_pedidos  valor_total_gbp ultima_compra
      14646    Netherlands             72        279138.02    2011-12-08
      18102 United Kingdom             60        259657.30    2011-12-09
      17450 United Kingdom             46        194390.79    2011-12-01
      14911        Ireland            198        136161.83    2011-12-08
      14156        Ireland             54        116560.08    2011-11-30
      17511 United Kingdom             31         91062.38    2011-12-07
      16029 United Kingdom             62         72708.09    2011-11-01
      16684 United Kingdom             28         66653.56    2011-12-05
      13694 United Kingdom             50         65039.62    2011-12-06
      15311 United Kingdom             91         60632.75    2011-12-09
```

## Taxa cancelamento pais

```
           pais  pedidos  cancelados  taxa_cancelamento_pct
          Italy       49          14                  28.57
    Switzerland       68          18                  26.47
          Japan       25           6                  24.00
        Germany      578         135                  23.36
         Poland       24           5                  20.83
Channel Islands       31           6                  19.35
         Sweden       42           8                  19.05
        Ireland      347          65                  18.73
      Australia       67          11                  16.42
        Belgium      117          19                  16.24
```

## Produtos mais devolvidos

```
codigo_produto                           descricao  quantidade_vendida  total_devolvido
         23843         PAPER CRAFT , LITTLE BIRDIE               80995            80995
         23166      MEDIUM CERAMIC TOP STORAGE JAR               78033            74494
         84347 ROTATING SILVER ANGELS T-LIGHT HLDR                9461             9376
         21108  FAIRY CAKE FLANNEL ASSORTED COLOUR               10241             3150
        85123A  CREAM HANGING HEART T-LIGHT HOLDER               37933             2578
         21175       GIN AND TONIC DIET METAL SIGN               12207             2030
         22920                   HERB MARKER BASIL                3423             1527
         22273                FELTCRAFT DOLL MOLLY                4509             1447
        47566B              TEA TIME PARTY BUNTING                2578             1424
         15034          PAPER POCKET TRAVELING FAN                6591             1385
```
