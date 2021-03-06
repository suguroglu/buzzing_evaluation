table_config = {"eval_final":{"stats_table":"su_buzzing_stats_comparison","stats_table_100":"su_final_reconciliation_top_100","pdf_table":"su_metrics_final_output"},"eval_intermediate":{"pdf_table":"su_buzzing_intermediate_metrics","i_hits_comparison":"su_buzzing_i_hits_comparison"}}
PROD_FINAL_OUTPUT_LOC = 's3://hearstkinesisdata/v4/'
DEV_FINAL_OUTPUT_LOC = 's3://hearstkinesisdata/v5/'
PROD_INTERMEDIATE_OUTPUT_LOC = 's3://hearstkinesisdata/processedsparkjsonix/'
DEV_INTERMEDIATE_OUTPUT_LOC = "s3://hearstdataservices/buzzing/parselyjson/"

FINAL_OUTPUT_LOG_FOLDER="final_out/"
HITS_TABLE_NAME = "i_hits"
site_mappings = {10004: 'greenwichtime.com', 10005: 'dariennewsonline.com', 10006: 'fairfieldcitizenonline.com',
                 10008: 'newcanaannewsonline.com', 10009: 'newmilfordspectrum.com', 10011: 'westport-news.com',
                 10012: 'beaumontenterprise.com', 10013: 'mysanantonio.com', 10014: 'seattlepi.com', 10017: 'wcvb.com',
                 10018: 'wmur.com', 10019: 'mor-tv.com', 10020: 'wesh.com', 10021: 'cw18tv.com', 10022: 'wtae.com',
                 10023: 'wbaltv.com', 10024: 'wbal.com', 10025: '98online.com', 10026: 'wyff4.com', 10027: 'wpbf.com',
                 10028: 'wgal.com', 10029: 'wxii12.com', 10030: 'wlky.com', 10031: 'wmtw.com', 10032: 'mynbc5.com',
                 10033: 'kmbc.com', 10034: 'wisn.com', 10035: 'wlwt.com', 10036: 'wdsu.com', 10037: 'koco.com',
                 10038: 'kcci.com', 10039: 'ketv.com', 10040: 'wapt.com', 10041: '4029tv.com', 10042: 'kcra.com',
                 10043: 'my58.com', 10044: 'koat.com', 10045: 'kitv.com', 10046: 'ksbw.com', 10047: 'answerology.com',
                 10048: 'caranddriver.com', 10049: 'cosmopolitan.com', 10050: 'countryliving.com', 10051: 'elle.com',
                 10052: 'elledecor.com', 10054: 'esquire.com', 10055: 'goodhousekeeping.com',
                 10056: 'harpersbazaar.com', 10057: 'housebeautiful.com', 10058: 'marieclaire.com',
                 10060: 'popularmechanics.com', 10063: 'redbookmag.com', 10064: 'roadandtrack.com',
                 10065: 'seventeen.com', 10067: 'townandcountrymag.com', 10069: 'veranda.com', 10070: 'womansday.com',
                 10071: 'kaboodle.com', 1649: 'hearstmags.com', 1782: 'delish.com', 1845: 'houstonchronicle.com',
                 1887: 'sfchronicle.com', 1888: 'expressnews.com', 1915: 'allaboutsoap.co.uk', 1916: 'allaboutyou.com',
                 1917: 'bestdaily.co.uk', 1918: 'company.co.uk', 1919: 'cosmopolitan.co.uk', 1920: 'digitalspy.com',
                 1921: 'elleuk.com', 1922: 'elledecoration.co.uk', 1923: 'esquire.co.uk', 1924: 'fetcheveryone.com',
                 1925: 'goodhousekeeping.co.uk', 1926: 'handbag.com', 1927: 'harpersbazaar.co.uk',
                 1928: 'insidesoap.co.uk', 1929: 'marieclaire.co.uk', 1930: 'menshealth.co.uk', 1931: 'netdoctor.co.uk',
                 1932: 'realpeoplemag.co.uk', 1933: 'redonline.co.uk', 1935: 'reveal.co.uk', 1936: 'runnersworld.co.uk',
                 1937: 'sugarscape.com', 1938: 'triathletes-world-magazine.co.uk', 1939: 'womenshealthmag.co.uk',
                 1940: 'zest.co.uk', 1941: 'elle.it', 1942: 'elledecor.it', 1943: 'cosmopolitan.it',
                 1944: 'marieclaire.it', 1945: 'riders-online.it', 1946: 'cosmogirl.nl', 1947: 'cosmopolitan.nl',
                 1948: 'elle.nl', 1949: 'esquire.nl', 1950: 'menshealth.nl', 1951: 'fiscalert.nl', 1952: 'quotenet.nl',
                 1953: 'red.nl', 1954: 'ar-revista.com', 1955: 'casadiez.elle.es', 1956: 'crecerfeliz.es',
                 1957: 'elle.es/viajes', 1958: 'diezminutos.es', 1959: 'elle.es', 1960: 'elle.es/elledeco',
                 1961: 'emprendedores.es', 1962: 'fotogramas.es', 1963: 'micasarevista.com', 1964: 'nuevo-estilo.es',
                 1965: 'quemedices.diezminutos.es', 1966: 'quo.es', 1967: 'supertele.es',
                 1968: 'teleprograma.fotogramas.es', 1969: 'caranddriverthef1.com', 1984: 'womenshealthmag.nl',
                 1985: 'cosmopolitantv.es', 1992: 'peopleschoice.com', 1995: 'drozthegoodlife.com',
                 2008: 'the-wedding.jp', 2009: 'ellemaman.jp', 2010: 'kodomonokuni.info', 2011: 'michigansthumb.com',
                 2012: 'mrt.com', 2013: 'myplainview.com', 2014: 'ourmidland.com', 2015: 'theintelligencer.com',
                 2016: 'lmtonline.com', 2017: 'elle.com.tw', 2019: 'elleeten.nl', 2020: 'elle.co.jp',
                 2021: 'ellegirl.jp', 2022: 'elleshop.jp', 2023: 'hearst.co.jp', 2024: '25ans.jp', 2025: 'mensclub.jp',
                 2026: 'trip.kyoto.jp', 2027: 'harpersbazaar.jp', 2028: 'cosmobody.com', 2030: 'mcchina.com',
                 2031: 'psychologies.com.cn', 2032: 'elleshop.com.cn', 2034: 'cad.com.cn', 2035: 'femina.com.cn',
                 2036: 'hearst.com.cn', 2059: 'countryliving.co.uk', 2060: 'housebeautiful.co.uk', 2063: 'ctnews.com',
                 2065: 'hearstdigital.com', 2068: 'wvtm13.com', 2069: 'cosmopolitan.ng', 2071: 'cosmopolitan.dk',
                 2072: 'cosmopolitan.no', 2077: 'cosmopolitan.se', 2078: 'wjcl.com', 2081: 'harpersbazaar.nl',
                 2084: 'southeasttexas.com', 2087: 'motor.com', 2088: '7yearsyounger.com', 2089: 'bestproducts.com',
                 2090: 'lennyletter.com', 2094: 'wearesweet.co', 2095: 'prima.co.uk', 2096: 'cosmopolitan.in',
                 2097: 'cosmopolitan-jp.com', 2098: 'cosmopolitan.com.tw', 2100: 'gioia.it',
                 2101: 'harpersbazaar.com.tw', 2104: 'womansdayspain.es', 2105: 'thehour.com', 2108: 'hearst.com',
                 2109: 'harpersbazaar.es', 2111: 'sellittexas.com', 2112: 'yourconroenews.com',
                 2114: 'townandcountrymag.co.uk', 2115: 'khtvnews.com', 4: 'sfgate.com', 5: 'stamfordadvocate.com',
                 6: 'ctpost.com', 7: 'chron.com', 8: 'newstimes.com', 9: 'timesunion.com'}



LIMIT = 1000
EPSILON = 40

SITE_THRESHOLD = 50
PERCENTAGE_THRESHOLD = 30