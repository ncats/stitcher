#!/usr/bin/env python3

import os
import io
import sys
import time
import json
import string
import urllib.request, urllib.error, urllib.parse
import zipfile
import gzip
import numpy
import re

resolverCache = dict()
resolverCache["PEMETREXED SODIUM"] = "2PKU919BA9"
resolverCache["NETARSUDIL DIMESYLATE"] = "VL756B1K0U"
resolverCache["INSULIN LISPRO RECOMBINANT"] = "GFX7QIS1II"
resolverCache["INSULIN SUSP ISOPHANE RECOMBINANT HUMAN"] = "1Y17CTI5SR"
resolverCache["PITAVASTATIN MAGNESIUM"] = "M5681Q5F9P"
resolverCache["IVACAFTOR, TEZACAFTOR"] = "8RW88Y506K"
resolverCache["NITROGEN, NF"] = "N762921K75"
resolverCache["ELEXACAFTOR, IVACAFTOR, TEZACAFTOR"] = "RRN67GMB0V"
resolverCache["GALLIUM DOTATOC GA-68"] = "Y68179SY2L"
resolverCache["GALLIUM DOTATATE GA-68"] = "9L17Y0H71P"
resolverCache["SIPONIMOD FUMARIC ACID"] = "Z7G02XZ0M6"
resolverCache["OMEGA-3ACID ETHYL ESTERS"] = "D87YGH4Z0Q"
resolverCache["OMEGA-3-ACID ETHYL ESTERS TYPE A"] = "D87YGH4Z0Q"
resolverCache["GRISEOFULVIN, ULTRAMICROSIZE"] = "32HRV3E3D5"
resolverCache["FISH OIL TRIGLYCERIDES"] = "XGF7L72M0F"
resolverCache["VERARD"] = "A7V27PHC7A" #https://books.google.com/books?id=IXWOXUpylO8C&pg=PA47&lpg=PA47&dq=verard+drug&source=bl&ots=WYI8qC1dDL&sig=ACfU3U3fGdeaopIKKyPWB3agXtKCHqNb9g&hl=en&sa=X&ved=2ahUKEwiwlc3pobnmAhVIwVkKHZ6lCIAQ6AEwCHoECAoQAQ#v=onepage&q=verard%20drug&f=false
resolverCache["COAGULATION FACTOR XA (RECOMBINANT), INACTIVATED - ZHZO"] = "BI009E452R"
resolverCache["FAM-TRASTUZUMAB DERUXTECAN-NXKI"] = "5384HK7574"
resolverCache["OMBITASVIR, PARITAPREVIR, RITONAVIR"] = "OU2YM37K86"
resolverCache["MYCOPHENOLIC SODIUM"] = "WX877SQI1G"


cberReplace = dict()
cberReplace['MUROMANAB-CD3'] = ['JGA39ICE2V']
cberReplace['SIMETHICONE'] = ['92RU3N3Y1O', 'ETJ7Z6XBU4']
cberReplace['CONJUGATED ESTROGENS/MEDROXYPROGESTERONE ACETATE'] = ['IU5QR144QX', 'C2QI4IOI2G']
cberReplace['LAMIVUDINE, NEVIRAPINE, AND STAVUDINE'] = ['2T8Q726O95', '99DK7FVK1H', 'BO9LE4QFZF']
cberReplace['IRBESARTAN: HYDROCHLOROTHIAZIDE'] = ['J0E2756Z7N', '0J48LPH2TH']
cberReplace['LINAGLIPTIN AND METFORMIN HYDROCHLORIDE'] = ['3X29ZEJ4R2', '786Z46389E']
cberReplace['TECHNETIUM TC99M ALBUMIN AGGREGATED'] = ['Z8E46IA45W']
cberReplace['ALOGLIPTIN AND METFORMIN HYDROCHLORIDE'] = ['JHC049LO86', '786Z46389E']
cberReplace['SAXAGLIPTIN HYDROCHLORIDE DIHYDRATE AND DAPAGLIFLOZIN'] = ['4N19ON48ZN', '1ULL0QJ8UC']
cberReplace['GALLIUM GA-68 PSMA-11'] = ['ZJ0EKR6M10']
cberReplace['ELAGOLIX SODIUM,ESTRADIOL,NORETHINDRONE ACETATE'] = ['5948VUI423', '4TI98Z838E', 'T18F433X4S']
cberReplace['GABAPENTIN ENCARBIL'] = ['75OCL1SPBQ']
cberReplace['CARBON DIOIDE'] = ['142M471B3J']
cberReplace['MENINGOCOCCAL POLYSACCHARIDE VACCINE, GROUPS A, C, Y, AND W-135 COMBINED'] = ['1I86B47NY4', '837RU6905N', 'CBZ4BH7TJ1', '9F8QQ6EER1']
cberReplace['MENINGOCOCCAL POLYSACCHARIDE VACCINE, GROUPS A AND C COMBINED'] = ['1I86B47NY4', '837RU6905N']
cberReplace['MENINGOCOCCAL POLYSACCHARIDE VACCINE, GROUP A'] = ['1I86B47NY4']
cberReplace['INSULIN ISOPHANE HUMAN'] = ['1Y17CTI5SR']
cberReplace['INSULIN ISOPHANE HUMAN AND INSULIN HUMAN'] = ['1Y17CTI5SR']
cberReplace['INSULIN ASPART PROTAMINE AND INSULIN ASPART'] = ['1Y17CTI5SR']
cberReplace['INFLUENZA VIRUS VACCINE'] = ['3872LLC68G', '3S1ZBQ2B9I', 'H41XP0E6G8']
cberReplace['DARATUMUMAB AND HYALURONIDASE-FIHJ'] = ['4Z63YK6E0E', '743QUY4VD8']
cberReplace['INSULIN DEGLUDEC AND INSULIN ASPART'] = ['54Q18076QB', 'D933668QVX']
cberReplace['HUMAN PAPILLOMAVIRUS BIVALENT (TYPES 16 AND 18) VACCINE, RECOMBINANT'] = ['6LTE2DNX63', 'J2D279PEM5']
cberReplace['LONCASTUXIMAB TESIRINE-LPYL'] = ['7K5O7P6QIU']
cberReplace['POSITIVE SKIN TEST CONTROL-HISTAMINE'] = ['820484N8I3']
cberReplace['POSITIVE SKIN TEST CONTROL - HISTAMINE'] = ['820484N8I3']
cberReplace['MENINGOCOCCAL POLYSACCHARIDE VACCINE, GROUP C'] = ['837RU6905N']
cberReplace['TECHNETIUM 99M TC FANOLESOMAB'] = ['AMF7KOE318']
cberReplace['INFLUENZA VIRUS VACCINE, H5N1'] = ['F3U797X68A', '3L2F8AM65K', 'UH52RJ06IG']
cberReplace['ATOLTIVIMAB, MAFTIVIMAB, AND ODESIVIMAB-EBGN'] = ['FJZ07Q63VY', 'KOP95331M4', 'UY9LQ8P6HW']
cberReplace['PEGINTERFERON ALFA-2B AND RIBAVIRIN'] = ['G8RGG88B68', '49717AWG6K']
cberReplace['PERTUZUMAB, TRASTUZUMAB, AND HYALURONIDASE-ZZXF'] = ['K16AIQ8CTM', 'P188ANX8CK', '743QUY4VD8']
cberReplace['TOSITUMOMAB AND IODINE I-131 TOSITUMOMAB'] = ['K1KT5M40JC']
cberReplace['INFLUENZA VACCINE LIVE, INTRANASAL'] = ['NY1FF92M1E', 'P8ORN3UOM6', 'B93BQX9789', 'VEH9U90EHX']
cberReplace['DOSTARLIMAB-GXLY'] = ['P0GVQ9A4S5']
cberReplace['PEGINTERFERON ALFA-2A AND RIBAVIRIN'] = ['Q46947FE7K', '49717AWG6K']
cberReplace['INFLUENZA A (H5N1) VIRUS MONOVALENT VACCINE, ADJUVANTED'] = ['TH23C7H4M5']
cberReplace['MODIFIED VACCINIA ANKARA - BAVARIAN NORDIC (MVA-BN)'] = ['TU8J357395']
cberReplace['EBOLA ZAIRE VACCINE, LIVE'] = ['Y9VG7O3KTT']
cberReplace['RADIOLABELED ALBUMIN TECHNETIUM TC-99M ALBUMIN COLLOID KIT'] = ['Z8E46IA45W']
cberReplace['ADENOVIRUS TYPE 4 AND TYPE 7 VACCINE, LIVE, ORAL'] = ['FKD3DUK39I', 'TM54L796SN']
cberReplace['ALBUMIN (HUMAN)-KJDA'] = ['ZIF514RVZR']
cberReplace['ALLOGENEIC CULTURED KERATINOCYTES AND FIBROBLASTS IN BOVINE COLLAGEN'] = ['T34C307W5N', 'FHJ3ATL51C', 'ZJO8CP3Q2A']
cberReplace['ANTHRAX IMMUNE GLOBULIN INTRAVENOUS (HUMAN)'] = ['VKZ83S945Z']
cberReplace['ANTIHEMOPHILIC FACTOR (RECOMBINANT), FC FUSION PROTEIN'] = ['7PCM518YLR']
cberReplace['ANTIHEMOPHILIC FACTOR (RECOMBINANT), FULL LENGTH'] = ['P89DR4NY54']
cberReplace['ANTIHEMOPHILIC FACTOR (RECOMBINANT), PEGYLATED'] = ['5X3GF74R79']
cberReplace['ANTIHEMOPHILIC FACTOR (RECOMBINANT), PLASMA/ALBUMIN FREE'] = ['113E3Z3CJJ']
cberReplace['ANTIHEMOPHILIC FACTOR (RECOMBINANT), RAHF'] = ['U50VWW6XH6']
cberReplace['AUTOLOGOUS CULTURED CHONDROCYTES ON PORCINE COLLAGEN MEMBRANE'] = ['D5P3K3V822', 'I8442U2G7J']
cberReplace['BOTULISM ANTITOXIN HEPTAVALENT (A, B, C, D, E, F, G) - (EQUINE)'] = ['LE3J6I6DXP', 'VSK09VP4HL', 'X5I2P7E9TY', '30Y9N0SEBE', 'T95649SUV7', 'RJN8G983LQ', '943578J0XG']
cberReplace['C1 ESTERASE INHIBITOR SUBCUTANEOUS (HUMAN)'] = ['6KIC4BB60G']
cberReplace['CANDIDA ALBICANS SKIN TEST ANTIGEN FOR CELLULAR HYPERSENSITIVITY'] = ['4D7G21HDBC']
cberReplace['COAGULATION FACTOR IX (RECOMBINANT), FC FUSION PROTEIN'] = ['02E00T2QDE']
cberReplace['COAGULATION FACTOR X (HUMAN)'] = ['0P94UQE6SY']
cberReplace['COAGULATION FACTOR XIII A-SUBUNIT (RECOMBINANT)'] = ['NU23Q531G1']
cberReplace['COCCIDIOIDES IMMITIS SPHERULE-DERIVED SKIN TEST ANTIGEN'] = ['ITY7G7Q744']
cberReplace['CROTALIDAE POLYVALENT IMMUNE FAB (OVINE)'] = ['RBR61YAJ4V', 'IA6O0K772M', '7WZ1744G86', 'A4229A7019']
cberReplace['DARBEPOETIN ALPHA'] = ['15UQ94PT4P']
cberReplace['DENGUE TETRAVALENT VACCINE, LIVE'] = ['75KB2HPX5H', 'FH5SVG7GLC', 'RHT2Q37FYG', 'RS26HP5ND2']
cberReplace['DIPHTHERIA & TETANUS TOXOIDS & ACELLULAR PERTUSSIS VACCINE ADSORBED'] = ['K3W1N8YP13', 'IRH51QN26H', 'QSN5XO8ZSU', '8C367IY4EY', 'I05O535NV6']
cberReplace['DIPHTHERIA & TETANUS TOXOIDS ADSORBED'] = ['IRH51QN26H', 'K3W1N8YP13']
cberReplace['DIPHTHERIA AND TETANUS TOXOIDS AND ACELLULAR PERTUSSIS ADSORBED AND INACTIVATED POLIOVIRUS VACCINE'] = ['IRH51QN26H', 'K3W1N8YP13', '8C367IY4EY', 'I05O535NV6', 'QSN5XO8ZSU', '0LVY784C09', '23JE9KDF4R', '459ROM8M9M']
cberReplace['DIPHTHERIA AND TETANUS TOXOIDS AND ACELLULAR PERTUSSIS ADSORBED, INACTIVATED POLIOVIRUS AND HAEMOPHILUS B CONJUGATE (TETANUS TOXOID CONJUGATE) VACCINE'] = ['IRH51QN26H', 'K3W1N8YP13', 'F4TN0IPY37', '8C367IY4EY', '63GD90PP8X', '1O0600285A', '0LVY784C09', '23JE9KDF4R', '459ROM8M9M', 'FLV5I5W26R']
cberReplace['DIPHTHERIA AND TETANUS TOXOIDS AND ACELLULAR PERTUSSIS VACCINE ADSORBED, INACTIVATED POLIOVIRUS, HAEMOPHILUS B CONJUGATE [MENINGOCOCCAL PROTEIN CONJUGATE] AND HEPATITIS B [RECOMBINANT] VACCINE'] = ['IRH51QN26H', 'K3W1N8YP13', 'F4TN0IPY37', '8C367IY4EY', '63GD90PP8X', '1O0600285A', '0LVY784C09', '23JE9KDF4R', '459ROM8M9M', 'XL4HLC6JH6', 'LUY6P8763W', 'F92V3S521O', 'F41V936QZM']
cberReplace['DTAP & HEPATITIS B (RECOMBINANT) & INACTIVATED POLIO VIRUS VACCINE'] = ['IRH51QN26H', 'K3W1N8YP13', 'QSN5XO8ZSU', '8C367IY4EY', 'I05O535NV6', '9GCJ1L5D1P', '0LVY784C09', '23JE9KDF4R', '459ROM8M9M']
cberReplace['FIBRIN SEALANT (HUMAN)'] = ['N94833051K', '6K15ABL77G']
cberReplace['FIBRIN SEALANT (TISSEEL)'] = ['N94833051K', '6K15ABL77G']
cberReplace['FIBRIN SEALANT PATCH'] = ['6K15ABL77G', 'N94833051K']
cberReplace['HAEMOPHILUS B CONJUGATE VACCINE (TETANUS TOXOID CONJUGATE)'] = ['FLV5I5W26R']
cberReplace['HEMATOPOIETIC PROGENITOR CELLS, CORD (HPC-C)'] = ['XU53VK93MC']
cberReplace['HEMIN FOR INJECTION'] = ['743LRP9S7N']
cberReplace['HEPATITIS A INACTIVATED & HEPATITIS B (RECOMBINANT) VACCINE'] = ['5BFC8LZ6LQ', '9GCJ1L5D1P']
cberReplace['HEPATITIS A VACCINE INACTIVATED'] = ['5BFC8LZ6LQ']
cberReplace['HEPATITIS B VACCINE (RECOMBINANT), ADJUVANTED'] = ['XL4HLC6JH6']
cberReplace['HOUSE DUST MITES (DERMATOPHAGOIDES FARINAE AND DERMATOPHAGOIDES PTERONYSSINUS) ALLERGEN EXTRACT'] = ['57L1Z5378K', 'PR9U2YPF3Q']
cberReplace['HUMAN PAPILLOMAVIRUS 9-VALENT VACCINE, RECOMBINANT'] = ['61746O90DY', 'Z845VHQ61P', '6LTE2DNX63', 'J2D279PEM5', '53JIL371NS', '759RAC446C', '68S8VCN34F', '55644W68FD', '94Y15HP7LF', 'F41V936QZM']
cberReplace['HUMAN PAPILLOMAVIRUS QUADRIVALENT (TYPES 6, 11, 16 AND 18) VACCINE, RECOMBINANT'] = ['61746O90DY', 'Z845VHQ61P', '6LTE2DNX63', 'J2D279PEM5', 'F41V936QZM']
cberReplace['HYALURONIDASE HUMAN'] = ['743QUY4VD8']
cberReplace['IMMUNE GLOBULIN INFUSION (HUMAN), 10% WITH RECOMBINANT HUMAN HYALURONIDASE'] = ['66Y330CJHS', '743QUY4VD8']
cberReplace['IMMUNE GLOBULIN INFUSION (HUMAN)'] = ['66Y330CJHS']
cberReplace['IMMUNE GLOBULIN INJECTION (HUMAN) 10% CAPRYLATE/CHROMATOGRAPHY PURIFIED'] = ['66Y330CJHS']
cberReplace['IMMUNE GLOBULIN INTRAVENOUS (HUMAN)-IFAS'] = ['66Y330CJHS']
cberReplace['IMMUNE GLOBULIN INTRAVENOUS (HUMAN), 10% LIQUID'] = ['66Y330CJHS']
cberReplace['IMMUNE GLOBULIN INTRAVENOUS, HUMAN-SLRA'] = ['66Y330CJHS']
cberReplace['IMMUNE GLOBULIN SUBCUTANEOUS (HUMAN)-HIPP'] = ['66Y330CJHS']
cberReplace['IMMUNE GLOBULIN SUBCUTANEOUS (HUMAN), 20% SOLUTION'] = ['66Y330CJHS']
cberReplace['INFLUENZA VACCINE, ADJUVANTED'] = ['XW4JB03TI5', '3NZW5ND3D6', '8V4458342X', '2J002Y0B0W', 'P8ORN3UOM6', 'INT614PB1A', 'G1T4TD4PZC']
cberReplace['INFLUENZA VACCINE'] = ['K8A11W1ZVV', '08R56E092Z', '0LS66T074T', 'H2198F8ZNA']
cberReplace['INSULIN DEGLUDEC AND LIRAGLUTIDE'] = ['54Q18076QB', '839I73S42A']
cberReplace['INSULIN GLARGINE AND LIXISENATIDE'] = ['2ZM8CX04RZ', '74O62BB01U']
cberReplace['INSULIN LISPRO PROTAMINE AND INSULIN LISPRO'] = ['GFX7QIS1II']
cberReplace['INTERFERON ALFA-N3 (HUMAN LEUKOCYTE DERIVED)'] = ['47BPR3V3MP']
cberReplace['IODINATED I-125 ALBUMIN'] = ['68WQQ3N9TI']
cberReplace['IODINATED I-131 ALBUMIN'] = ['ACH35131L1']
cberReplace['JAPANESE ENCEPHALITIS VACCINE, INACTIVATED, ADSORBED'] = ['DZ854I04ZE']
cberReplace['MEASLES, MUMPS AND RUBELLA VIRUS VACCINE LIVE'] = ['MFZ8I7277D', '47QB6MX9KU', '52202H034Z']
cberReplace['MEASLES, MUMPS, RUBELLA AND VARICELLA VIRUS VACCINE LIVE'] = ['MFZ8I7277D', '47QB6MX9KU', '52202H034Z', 'GPV39ZGD8C']
cberReplace['MENINGOCOCCAL (GROUPS A, C, Y AND W-135) POLYSACCHARIDE DIPHTHERIA TOXOID CONJUGATE VACCINE'] = ['RE9A0H8OAB', '2J57K2523T', 'MI340WV90B', 'Z6R9D1D3KJ']
cberReplace['MENINGOCOCCAL (GROUPS A, C, Y, AND W-135) OLIGOSACCHARIDE DIPHTHERIA CRM197 CONJUGATE VACCINE'] = ['3O44U6XYQK', 'H2W22AGF1P', '5JT3N61JSP', '2W566Z2PEJ']
cberReplace['MENINGOCOCCAL (GROUPS A, C, Y, W) CONJUGATE VACCINE'] = ['T4GYX3110D', 'ZT89E5A103', '4WAN8PQK15', 'L77OK410KW']
cberReplace['MENINGOCOCCAL GROUP B VACCINE'] = ['28E911Y7AE', '25DB599G64', '1S25R442RS', '91523M4S24']
cberReplace['PNEUMOCOCCAL 13-VALENT CONJUGATE VACCINE (DIPHTHERIA CRM197 PROTEIN)'] = ['54EC0SE5PZ', '2VF3V7175U', 'TGJ6YZC4W7', '5SKG37872O', 'Z9HK08690W', '4M543JVT7G', '0K0S2P98ZJ', '5Q768OY0GI', 'SK54I0S386', 'XK87D9J012', '25N8E57V6T', 'B970MQT365', '2E1M7F958B']
cberReplace['PNEUMOCOCCAL VACCINE, POLYVALENT'] = ['H9NOI61UH1', 'E11P4F3X4S', '4FVB62AFF1', 'CGS5KI3Q2M', 'N8R9GL539D', '57F7254B6Q', 'X1K54R2P9I', '669818346F', '313LJP87ET', 'DL82PE6ANE', '328VNB72T8', 'N967BGT6XW', 'S46U1CM432', 'G1GFK9898U', '667Y1EG6EW', '3RED79E75R', '23036553F6', '5K0VU709JD', '9W2T4OSF98', 'V3FC0DK9XS', '7NLV25LOSI', 'PYD255827T', '2MGG3XW1L2']
cberReplace['POLIOVIRUS VACCINE INACTIVATED (HUMAN DIPLOID CELL)'] = ['08GY9K1EUO']
cberReplace['POLIOVIRUS VACCINE INACTIVATED'] = ['0LVY784C09', '23JE9KDF4R', '459ROM8M9M']
cberReplace['POOLED PLASMA (HUMAN), SOLVENT/DETERGENT TREATED'] = ['6D53G0FD0Z']
cberReplace['PROTHROMBIN COMPLEX CONCENTRATE (HUMAN)'] = ['8FB1K07F16', '4156XVB4QD', '6U90Y1795T', '0P94UQE6SY', '3Z6S89TXPW', '90J3F6N5FN']
cberReplace['RHO(D) IMMUNE GLOBULIN INTRAVENOUS (HUMAN)'] = ['66Y330CJHS']
cberReplace['RITUXIMAB AND HYALURONIDASE HUMAN'] = ['4F4X42SYQ6', '8KOG53Z5EM']
cberReplace['ROTAVIRUS VACCINE, LIVE, ORAL, PENTAVALENT'] = ['25VC15141Q', 'JU499IS53H', '236YGP181O', '6334XMP4KC', 'L1977Q86S5']
cberReplace['ROTAVIRUS VACCINE, LIVE, ORAL'] = ['KZ3L01D2PC']
cberReplace['SHORT RAGWEED POLLEN ALLERGEN EXTRACT'] = ['K20Y81ACO3']
cberReplace['SWEET VERNAL, ORCHARD, PERENNIAL RYE, TIMOTHY, AND KENTUCKY BLUE GRASS MIXED POLLENS ALLERGEN EXTRACT'] = ['2KIK19R45Y', '83N78IDA7P', '4T81LB52R0', '65M88RW2EG', 'SCB8J7LS3T']
cberReplace['TETANUS AND DIPHTHERIA TOXOIDS ADSORBED'] = ['751E8J54VM', '3U7E3O07S8', 'K3W1N8YP13', 'IRH51QN26H']
cberReplace['TETANUS TOXOID, REDUCED DIPHTHERIA TOXOID AND ACELLULAR PERTUSSIS VACCINE, ADSORBED'] = ['K3W1N8YP13', 'IRH51QN26H', 'QSN5XO8ZSU', '8C367IY4EY', 'I05O535NV6']
cberReplace['THIN-LAYER RAPID USE EPICUTANEOUS PATCH TEST'] = ['JC9WZ4FK68', '884C3FA9HE', '057Y626693', 'T4423S18FM', 'U3RSY48JW5', '5NF5D4OPCI', 'Z97702A5DG', 'L837108USY', 'SR60A3XG0F', 'SS8YOP444F', '8SQ0VA4YUR', '3T8H1794QW', '5M0MWY797U', 'WC51CA3418', 'O3034Q5AHK', '88S87KL877', 'Z8IX2SC1OH', 'A2I8C7HI9T', '14255EXE39', '3QPI1U3FV8', '8Y41DYV4VG', '8P5F881OCY', '60V9STC53F', '17AVG63ZBC', 'Q51LPW21CH', 'F3XRM1NX4H', '6MRZ85RNHQ', 'ICW4708Z8G', 'HNM5J934VP', '0M7PSL4100', 'T29JGK5V4R', 'DD517SCM92', 'DEL7T5QRPN', 'E40U03LEM0', 'YX089CPS05', 'U770QIT64J', '1HG84L3525', 'UCA53G94EV', '6OK753033Z', 'VCD7623F3K', '2225PI3MOV', '01W430XXSQ', '0D771IS0FH', 'TR3MLJ1UAI', 'CR113982E5', 'H5RIZ3MPW4', '7BHQ856EJ5', 'D6VHC87LLS', '6K28E35M3B', 'CKS1YQ9W1J', 'M629807ATL', 'Q3OKS62Q6X', '05RMF7YPWN', '5RLR54Z22K', '58H6RWO52I', '2RDB26I5ZB', 'C48O4QYD6N', '6PU1E16C9W']
cberReplace['THROMBIN TOPICAL (RECOMBINANT)'] = ['SCK81AMR7R']
cberReplace['THROMBIN, TOPICAL (BOVINE)'] = ['25ADE2236L']
cberReplace['TIMOTHY GRASS POLLEN ALLERGEN EXTRACT'] = ['65M88RW2EG']
cberReplace['TRASTUZUMAB AND HYALURONIDASE-OYSK'] = ['P188ANX8CK', '743QUY4VD8']
cberReplace['VENOMS, MIXED VESPID VENOM PROTEIN'] = ['J8DAZ3T66L', '7PI26E943G', '8SH7583MUK', 'V34908RT03', 'Q79PS8P34R', 'D7974DM2EJ', 'S125N1F5X5']
cberReplace['VENOMS, WASP VENOM PROTEIN'] = ['987GS3GJZX', 'L0L5D5D9BQ', 'AKT0E6058K', 'ZN217W5Y50']
cberReplace['VENOMS, YELLOW JACKET VENOM PROTEIN'] = ['8SH7583MUK', 'V34908RT03', 'Q79PS8P34R', 'D7974DM2EJ', 'S125N1F5X5']
cberReplace['ZOSTER VACCINE LIVE'] = ['GPV39ZGD8C', '059QF0KO0R']
cberReplace['ZOSTER VACCINE RECOMBINANT, ADJUVANTED'] = ['COB9FF6I46']
cberReplace['TEZEPELUMAB-EKKO'] = ['RJ1IW3B4QX']
cberReplace['EFGARTIGIMOD ALFA-FCAB'] = ['961YV2O515']
cberReplace['INSULIN GLARGINE-AGLR'] = ['2ZM8CX04RZ']
cberReplace['ADALIMUMAB-AQVH'] = ['FYS6T7F842']
cberReplace['TRALOKINUMAB-LDRM'] = ['GK1LYB375A']
cberReplace['ALLOGENEIC CULTURED KERATINOCYTES AND DERMAL FIBROBLASTS IN MURINE COLLAGEN-DSAT'] = ['DYB2T93463']
cberReplace['COVID-19 VACCINE, MRNA'] = ['5085ZFP6SJ']
cberReplace['SMALLPOX (VACCINIA) VACCINE, LIVE'] = ['4SV59689SK']
cberReplace['PLASMINOGEN, HUMAN-TVMH'] = ['1EF190B6M7']
cberReplace['SMALLPOX AND MONKEYPOX VACCINE, LIVE, NON-REPLICATING'] = ['TU8J357395']
cberReplace['ALLOGENEIC PROCESSED THYMUS TISSUE'] = ['XD66YK3YY3']
cberReplace['INFLUENZA A (H5N1) MONOVALENT VACCINE, ADJUVANTED'] = ['TH23C7H4M5']
cberReplace['20-VALENT PNEUMOCOCCAL CONJUGATE VACCINE'] = ['ADR2S9OIF2','N47C1SHV0F','RDA5AKV23D','T6O227OX7Q','H8Q6FRO6DY','U1E9VSB2K2','9WP2BC3I04']
cberReplace['PNEUMOCOCCAL 15-VALENT CONJUGATE VACCINE'] = ['08VC9WC084']

def getStitcherDataInxightRepo():
    stitcherDataInxightRepo = "../stitcher-data-inxight"
    return stitcherDataInxightRepo

def getTimeStamp():
    ts = time.gmtime()
    return time.strftime("%Y-%m-%d", ts)

def getOBZipURL():
    #zipurl = "https://www.fda.gov/downloads/Drugs/InformationOnDrugs/UCM163762.zip"
    zipurl = "https://www.fda.gov/media/76860/download"
    return zipurl

def getUNIIZipURL():
    zipurl = "https://fdasis.nlm.nih.gov/srs/download/srs/UNIIs.zip"
    return zipurl

def getDrugsFDAZipURL():
    #zipurl = "https://www.fda.gov/downloads/Drugs/InformationOnDrugs/UCM527389.zip"
    zipurl = "https://www.fda.gov/media/89850/download"
    #page = urllib2.urlopen('https://www.fda.gov/Drugs/InformationOnDrugs/ucm079750.htm').read()
    #i = page.find('Drugs@FDA Download File')
    #j = page.rfind('href="/downloads/Drugs/InformationOnDrugs/', 0, i)
    #k = page.find('\"', j+6)
    #zipurl = "https://www.fda.gov/"+page[j+6:k]
    return zipurl

def getPurpleBookURL():
    ts = time.gmtime(time.mktime(time.gmtime())-60*60*24*31) # look one month back to increase chance of getting valid download
    year = time.strftime("%Y", ts)
    month = time.strftime("%B", ts).lower()
    purpleBookURL = 'https://purplebooksearch.fda.gov/files/%s/purplebook-search-%s-data-download.csv' % (year, month)
    return purpleBookURL

def getMainDir():
    curr = os.getcwd()
    if curr[-8:] == '/scripts':
        curr = curr[:-8]
    if curr[-5:] == '/temp':
        curr = curr[:-5]

    if not os.path.exists(curr+"/.git") or not os.path.exists(curr+"/data") or not os.path.exists(curr+"/scripts"):
        raise ValueError('Could not identify repo head from current directory')

    if not os.path.exists(curr+"/temp"):
        os.mkdir(curr+"/temp")

    return curr

def resolveName(name): # returns empty string if resolver can't do anything with this name
    if name in resolverCache.keys():
        return resolverCache[name]

    try:
        unii = resolveNameTripod(name)
        if len(unii) < 10:
            if name[-4:] == " KIT": # TECHNETIUM TC-99M TEBOROXIME KIT
                sname = name[:-4]
                unii = resolveNameTripod(sname)
            elif name[-5:-4] == "-": # PEGFILGRASTIM-JMDB
                sname = name[:-5]
                unii = resolveNameTripod(sname)
        resolverCache[name] = unii
        return unii
    except:
        raise
        return ''

def resolveNameTripod(name):
    unii = ''
    #response = urllib2.urlopen("https://tripod.nih.gov/servlet/resolverBeta3/unii", "structure="+name+"&format=json&apikey=5fd5bb2a05eb6195").read()
    #r = json.loads(response)
    #if len(r) > 0 and r[0].has_key('response'):
    #    resolverCache[name] = r[0]['response']
    #    if r[0]['input'] == unicode(name) and r[0]['source'] == unicode('FDA-SRS') and len(r[0]['response']) == 10:
    #        return r[0]['response']
    #else:

    request = "https://tripod.nih.gov/ginas/app/api/v1/substances/search?q=root_names_name:\"^"+urllib.parse.quote(name)+"$\""
    response = "{\"total\":0}"
    try:
        response = urllib.request.urlopen(request).read()
    except:
        response
    r = json.loads(response)
    if int(r['total']) > 0:
        for i in range(0, int(r['total'])):
            if '_approvalIDDisplay' in r['content'][i]:
                if unii == '':
                    unii = r['content'][i]['_approvalIDDisplay']
                elif unii != r['content'][i]['_approvalIDDisplay']:
                    raise ValueError("conflicting UNIIs returned:"+name+":"+resolverCache[name]+":"+request)
            elif 'approvalID' in r['content'][i]:
                if unii == '':
                    unii = r['content'][i]['approvalID']
                elif unii != r['content'][i]['approvalID']:
                    raise ValueError("conflicting UNIIs returned:"+name+":"+resolverCache[name]+":"+request)
            else:
                raise ValueError("UNIIs response not properly formatted:"+name)
    return unii

def parseIngred(ing, uniiPT, uniiALL, missing):
    ingreds = []
    ing = ing.strip().upper()
    if ing not in uniiPT:
        ing = ing + '; '
        while ing.find('; ') > -1:
            si = ing[:ing.find('; ')].strip()
            if si in uniiPT:
                ingreds.append(uniiPT[si])
            elif si in uniiALL:
                ingreds.append(uniiALL[si])
            elif si.find(';') > si.find('(') and si.find(';') < si.find(')') and si.find(')') == len(si)-1: #TRIPLE SULFA (SULFABENZAMIDE;SULFACETAMIDE;SULFATHIAZOLE)
                ing = ing[len(si):]
                si = si[si.find('(')+1:-1]
                si = si.replace(';', '; ')
                ing = '; ' + si + ing
                si = ''
            elif si.find(';') > -1: #ATAZANAVIR SULFATE;RITONAVIR;LAMIVUDINE;ZIDOVUDINE
                ing = ing[len(si):]
                si = si.replace(';', '; ')
                ing = '; ' + si + ing
                si = ''
            elif si in cberReplace:
                for unii in cberReplace[si]:
                    ingreds.append(unii)
            else:
                match = resolveName(si)
                if len(match) == 10:
                    ingreds.append(match)
                else:
                    if si not in missing:
                        sys.stderr.write("Ingredient does not map to UNII:"+si+"\t"+NDA+"\n")
                        missing[si] = []
                    missing[si].append(NDA)
            ing = ing[ing.find('; ')+2:]
        if len(ing) > 0 and ing not in uniiPT:
            raise ValueError('Problem parsing ingredients:'+ing)
    else:
        ingreds.append(uniiPT[ing])
    return ingreds

def readTabFile(filename, header = True, delim = '\t'):
    fp = open(filename, 'r')
    data = readTabFP(fp, header, delim)
    fp.close()
    return data

def carefulSplit(line, delim):
    sline = []
    inquote = 0
    j = 0
    for i in range(len(line)):
        if inquote > 0:
            if line[i] == '"':
                inquote = 0
        else:
            if line[i] == '"':
                inquote = 1
            if line[i] == delim:
                if line[j] == '"' and line[i-1] == '"':
                    sline.append(line[j+1:i-1])
                else:
                    sline.append(line[j:i])
                j = i + 1
    sline.append(line[j:])
    return sline

def readTabFP(fp, header = True, delim = '\t'):
    data = dict()
    line = fp.readline()
    eol = -1
    if line[-2] == '\r' or line[-2] == '\n':
        eol = -2
    elems = 0
    if header:
        sline = carefulSplit(line[:eol], delim)
        data['header'] = sline
        elems = len(sline)
        line = fp.readline()
    data['table'] = []
    while line != "":
        sline = carefulSplit(line[:eol], delim)
        if len(sline) < elems:
            elems = len(sline)
        data['table'].append(sline)
        line = fp.readline()

    if header:
        while len(data['header']) < elems:
            data['header'].append('')
    for i in range(len(data['table'])):
        while len(data['table'][i]) < elems:
            data['table'][i].append('')

    return data

def writeInitApp(outfp, unii, early, earlyDate, myunii):
    date = ''
    year = ''
    if len(earlyDate) > 0:
        date = earlyDate[5:7]+"/"+earlyDate[8:]+"/"+earlyDate[0:4]
        year = date[-4:]
    #['072437/001', 'FENOPROFEN CALCIUM', 'CAPSULE;ORAL', 'EQ 200MG BASE', 'Discontinued', 'ANDA', 'PAR PHARM', '1988-08-22', 'Drugs@FDA']
    method = early[-1]
    apptype = early[-4]
    appsponsor = early[-3]
    product = early[-8]
    appno = ''
    try:
        appno = str(int(early[0][0:6]))
        while len(appno) < 6:
            appno = "0" + appno
    except:
        appno = ''

    appurl = ''
    active = "true"
    if early[-5] != "Prescription" and early[-5] != "Over-the-counter":
        active = "false"

    if early[-1].find('https://www.accessdata.fda.gov/') > -1:
        #appno = early[0][0:6]

        appurl = early[-1]
        method = "Drugs@FDA"
    elif len(early[0]) > 6 and early[0][6] == '/':
        #appno = early[0][0:6]

        if method == "PREDICTED":
            year = "[Approval Date Uncertain] "+year

        appurl = "https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo="+appno
    else:
        #outfp = fperr
        active = "false"
        if method.find('OB NME Appendix 1950-') > -1:
            #appno = early[0][0:6]
            appno = appno
        elif method.find('http') > -1:
            appurl = method
            method = "Literature"
            
        for otherunii in activeMoiety[unii]:
            if otherunii in UNII2prods:
                for prod in UNII2prods[otherunii]:
                    entry = prods[prod]
                    if entry[0] == early[0]:
                        unii = otherunii

    #comment = apptype+'|'+appno+'|'+appsponsor+'|'+product+'|'+appurl+'|'
    #comment = comment + early[0].decode('latin-1').encode('ascii', 'replace')
    #outline = unii+"\tApproval Year\t"+year+"\t\t"+comment+"\t"+date+"\t"+method+"\n"
    comment = early[0]# .decode('latin-1').encode('ascii', 'replace')
    outline = myunii+"\t"+year+"\t"+date+"\t"+method+"\t"+apptype+"\t"+appno+"\t"+appsponsor+"\t"+product+"\t"+appurl+"\t"+active+"\t"+comment+"\n"
    outfp.write(outline)

def readUniiFile(maindir):
    uniifile = maindir+"/temp/UNIIs-"+getTimeStamp()+".zip"
    syscall = "curl --insecure -o "+uniifile + " " + getUNIIZipURL()
    print(syscall)

    if not os.path.exists(uniifile):
        os.system(syscall)
    
    zfp = zipfile.ZipFile(uniifile, 'r')
    names = zfp.namelist()
    fp = io.TextIOWrapper(zfp.open(names[-1], 'r'))
    line = fp.readline()

    if line[:-1].upper() != "NAME\tTYPE\tUNII\tDISPLAY NAME" and line[:-1].upper() != "NAME\tTYPE\tUNII\tPT":
        raise ValueError('Problem reading UNII file:'+line)

    line = fp.readline()
    uniiPT = dict()
    uniiALL = dict()

    while line != "":
        sline = line[:-1].split("\t")
        if len(sline) < 4:
            raise ValueError('Problem reading UNII fileline:'+line)
        uniiPT[sline[3]] = sline[2]
        if sline[0][-14:] == " [ORANGE BOOK]":
            sline[0] = sline[0][:-14]
        uniiALL[sline[0]] = sline[2]
        line = fp.readline()
    print("UNIIs in memory:", len(uniiPT), len(uniiALL))
    return uniiPT, uniiALL

def writeCBERBLAs(purpleBookfile, fdaSPLfile, fpout, uniiPT, uniiALL):
    spldata = dict()
    spldata['table'] = []
    #fp = open(fdaSPLfile, 'r')
    #spldata = readTabFP(fp)
    #fp.close()

    w = re.compile('\w+')
    dropphrases = ['KIT FOR THE PREPARATION OF ', 'KIT FOR ']

    cberBLAs = dict()

    # purple book download https://purplebooksearch.fda.gov/downloads
    print(purpleBookfile)
    fp = open(purpleBookfile, 'r')
    line = fp.readline()
    while not line.startswith('Purple Book Database Extract'):
        line = fp.readline()
    data = readTabFP(fp, True, ',')
    for item in data['table']:
        if len(item) < 10:
            print(item)
            sys.exit()
        ingred = item[4].upper()
        for phrase in dropphrases:
            if ingred.find(phrase) == 0:
                ingred = ingred[len(phrase):]
        uniis = []
        if ingred in cberReplace:
            uniis = cberReplace[ingred]
        elif ingred in uniiALL:
            uniis = [uniiALL[ingred]]
        else:
            for prod in spldata['table']:
                if prod[6] == 'BLA%06d' % (int(item[2])) or prod[6] == 'NDA%06d' % (int(item[2])):
                    if prod[10] in uniiALL and uniiALL[prod[10]] not in uniis:
                        uniis.append(uniiALL[prod[10]])
                    else: # bogus SPL entry, e.g. NDC 66521-200
                        nothing = True
                        #uniis.append('BOGUS')
            if len(uniis) > 0:
                cberReplace[ingred] = uniis
                print("cberReplace['%s'] = ['%s']" % (ingred, "', '".join(uniis)))
            else:
                print("Unrecognized purple book entry: %s" % (ingred))
                cberReplace[ingred] = []
                uniis = []
        product = item[3]
        brand = item[3]
        if len(brand) == 0:
            brand = product
        manu = item[1]
        bla = item[2]
        date = item[20] if len(item[20]) > 0 and int(item[20][-4:]) < int(item[12][-4:]) else item[12]
        if date != '' and date[-4:] < "1930":
            print("Replaced bogus date:" + manu + " " + brand + " " + date)
            date = date[0:-4] + '1984'
        form = item[7]
        route = item[8]
        comment = product + " " + route + " " + form
        active = "true"
        if item[10] == "Disc":
            active = "false"
            comment = comment + " Discontinued"
        url = "https://purplebooksearch.fda.gov/"

        if item[19] == 'CBER' and item[5] == '351(a)':
            for unii in uniis:
                # UNII	Approval_Year	Date	Date_Method	App_Type	App_No	Sponsor	Product	Url	active	Comment
                #R9400W927I	2000	04/25/2000	Drugs@FDA	ANDA	075581	TEVA	KETOCONAZOLE	https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=075581	true	075581/001
                entry = unii+"\t"+date[-4:]+"\t"+date+"\tCBER-BLAs\tBLA\t"+bla+"\t"+manu+"\t"+brand+"\t"+url+"\t"+active+"\t"+comment+"\n"
                if unii not in cberBLAs:
                    cberBLAs[unii] = []
                if entry not in cberBLAs[unii]:
                    cberBLAs[unii].append(entry)

    # approvalsList https://www.fda.gov/media/76363/download
    # approval list with BLAs https://www.fda.gov/media/113210/download
    # prodDates https://www.fda.gov/media/76356/download
    # Use Adobe Acrobat -> Export to text (plain)

    #Sort	Applicant / License #	Proprietary Name	Proper Name	BLA STN#	Product #	Total Drug Content (Concentration)	Approval Date	Dosage Form/Route/Presentation	Discontinued Date
    #1	ADMA BIOLOGICS INC / 2019	ASCENIV	"IMMUNE GLOBULIN INTRAVENOUS, HUMAN-SLRA 10%"	125590 / 0	1	5000 MG/50 ML ( 100 MG/ML )	04/01/2019	SOLUTION / INTRAVENOUS / SINGLE-DOSE VIAL	

    w = re.compile('\w+')
    fp = open(getMainDir()+"/scripts/data/User Fee Billable Biologic Products and Potencies Approved Under Section 351 of PHS  Act.txt", "r", encoding='cp1252')
    data = readTabFP(fp)
    fp.close()
    #print data['header']
    for item in data['table']:
        if len(item) < 10:
            print(item)
            sys.exit()
        for i in range(len(item)):
            if len(item[i]) > 2 and item[i][0] == '"' and item[i][-1] == '"':
                item[i] = item[i][1:-1]
        ingred = item[3]
        while len(ingred) > 5:
            if ingred in uniiALL or ingred in cberReplace:
                break
            matches = list(re.finditer(w, ingred))
            if len(matches) > 1:
                if ingred[matches[-2].span()[1]] == ')' or ingred[matches[-2].span()[1]] == '%':
                    ingred = ingred[:matches[-2].span()[1]+1]                   
                else:
                    ingred = ingred[:matches[-2].span()[1]]
            else:
                print("Unrecognized cber billable entry: %s" % (item[3]))
                ingred = ''
        uniis = []
        if ingred in cberReplace:
            uniis = cberReplace[ingred]
        elif ingred in uniiALL:
            uniis = [uniiALL[ingred]]
        product = item[3]
        brand = item[2]
        if len(brand) == 0:
            brand = product
        manu = item[1][:item[1].find(" / ")]
        bla = item[4][:item[4].find(" / ")]
        date = item[7]
        form = item[8][:item[8].find(" / ")]
        route = item[8][item[8].find(" / ")+3:item[8].rfind(" / ")]
        comment = product + " " + route + " " + form
        discndate = ""
        if len(item) > 9:
            discndate = item[9]
        active = "true"
        if len(discndate) > 0:
            active = "false"
            comment = comment + " Discontinued: " + discndate
        url = "https://www.fda.gov/media/113210/download"
        for unii in uniis:
            # UNII	Approval_Year	Date	Date_Method	App_Type	App_No	Sponsor	Product	Url	active	Comment
            #R9400W927I	2000	04/25/2000	Drugs@FDA	ANDA	075581	TEVA	KETOCONAZOLE	https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=075581	true	075581/001
            entry = unii+"\t"+date[-4:]+"\t"+date+"\tCBER-BLAs\tBLA\t"+bla+"\t"+manu+"\t"+brand+"\t"+url+"\t"+active+"\t"+comment+"\n"
            if unii != '':
                if unii not in cberBLAs:
                    cberBLAs[unii] = []
                if entry not in cberBLAs[unii]:
                    cberBLAs[unii].append(entry)
    #print len(cberBLAs)
    for unii in cberBLAs:
        for entry in cberBLAs[unii]:
            fpout.write(entry)

    initUniis = list(cberBLAs.keys())
    
    licTxt = open(getMainDir()+"/scripts/data/LicEstablishList.txt", "r", encoding='cp1252').readlines()
    url = "https://www.fda.gov/media/76356/download"
    
    idregex = re.compile('\d\d\d\d\s')
    dateregex = re.compile('\s\d\d-[A-S][A-Z][A-Z]-\d\d\d\d\s')

    i = 1
    while i < len(licTxt):
        idmatch = re.match(idregex, licTxt[i])
        id = idmatch.group(0)[:-1]
        name = licTxt[i][len(id)+1:-1]
        if re.search(dateregex, name):
            print("problem with name", name)
            sys.exit()
        #print id, name
        address = ''
        i = i + 1
        while (not re.findall(dateregex, licTxt[i])):
            address = address + licTxt[i]
            i = i + 1
        #print address
        prodstr = ''
        while i<len(licTxt) and not re.match(idregex, licTxt[i]):
            prodstr = prodstr + licTxt[i]
            i = i + 1
        prodstr = prodstr.replace("\n", " ").replace("\t", " ")
        datematch = re.findall(dateregex, prodstr)
        datematch.reverse()
        prods = []
        for match in datematch:
            if prodstr.rfind(match) > 0:
                prods.append(prodstr[prodstr.rfind(match):])
                prodstr = prodstr[:prodstr.rfind(match)]
        prods.append(prodstr)
        #print prods
        prods2 = [] #[date, product]
        for prod in prods:
            prods2.append([prod[1:12], prod[13:].strip()])
        #print prods2
        for prod in prods2:
            if prod[1].upper() in uniiALL:
                unii = uniiALL[prod[1].upper()]
                if unii not in initUniis:
                    # UNII	Approval_Year	Date	Date_Method	App_Type	App_No	Sponsor	Product	Url	active	Comment
                    #R9400W927I	2000	04/25/2000	Drugs@FDA	ANDA	075581	TEVA	KETOCONAZOLE	https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo=075581	true	075581/001
                    ts = time.strptime(prod[0], "%d-%b-%Y")
                    date = time.strftime("%m/%d/%Y", ts)
                    entry = unii+"\t"+prod[0][-4:]+"\t"+date+"\tCBER-BLAs\tBLA\t\t"+name+"\t"+prod[1]+"\t"+url+"\ttrue\t\n"
                    if unii not in cberBLAs:
                        cberBLAs[unii] = []
                    if entry not in cberBLAs[unii]:
                        cberBLAs[unii].append(entry)
                    fpout.write(entry)
            elif prod[0] == prod[1]:
                print("Something very wrong")
                sys.exit()

if __name__=="__main__":

    maindir = getMainDir()
    stitcherDataInxightRepo = getStitcherDataInxightRepo()

    drugsAtfdafile = maindir+"/temp/drugsAtfda-"+getTimeStamp()+".zip"
    syscall = "curl --insecure -o "+drugsAtfdafile + " " + getDrugsFDAZipURL()
    print(syscall)

    if not os.path.exists(drugsAtfdafile):
        os.system(syscall)

    obfile = maindir+"/temp/orangeBook-"+getTimeStamp()+".zip"
    syscall = "curl --insecure -o "+obfile + " " + getOBZipURL()
    print(syscall)
    if not os.path.exists(obfile):
        os.system(syscall)

    purpleBookfile = maindir+"/temp/purpleBook-"+getTimeStamp()+".csv"
    syscall = "curl --insecure -o "+purpleBookfile + " " + getPurpleBookURL()
    print(syscall)
    if not os.path.exists(purpleBookfile):
        os.system(syscall)

    gsrsDumpfile = maindir + f'/{stitcherDataInxightRepo}/files/dump-public-2021-12-15.gsrs'
    if not os.path.exists(gsrsDumpfile):
        raise ValueError("Can't find GSRS dump file for active moiety lookup: "+gsrsDumpfile)

    fdaNMEfile = maindir+'/scripts/data/FDA-NMEs-2018-08-07.txt'
    if not os.path.exists(fdaNMEfile):
        raise ValueError("Can't find FDA NMEs file for historical approval dates: "+fdaNMEfile)

    uniiPT, uniiALL = readUniiFile(maindir)

    fdaSPLfile = maindir+'/data/spl_summary.txt'
    if not os.path.exists(fdaSPLfile):
        raise ValueError("Can't find FDA SPLs file: "+fdaSPLfile)

    zfp = zipfile.ZipFile(drugsAtfdafile, 'r')
    fp = io.TextIOWrapper(zfp.open('Products.txt', 'r'))
    #ApplNo\tProductNo\tForm\tStrength\tReferenceDrug\tDrugName\tActiveIngredient\tReferenceStandard
    #0       1          2     3         4              5         6                 7
    line = fp.readline()
    if line.find("ApplNo\tProductNo\tForm\tStrength\tReferenceDrug\tDrugName\tActiveIngredient") != 0:
        raise ValueError('Problem reading Products file:'+line)
    line = fp.readline()
    UNII2prods = dict()
    prods = dict() # prods[NDA/part no] = [NDA/part, prodName, form, strength, status, app type, sponsor, year, ref]
    #['072437/001', 'FENOPROFEN CALCIUM', 'CAPSULE;ORAL', 'EQ 200MG BASE', 'Discontinued', 'ANDA', 'PAR PHARM', '1988-08-22', 'Drugs@FDA']

    missing = dict()
    while line != "":
        sline = line[:-1].split("\t")
        if len(sline) < 4:
            raise ValueError('Problem reading Products file2:'+line)
        NDA = sline[0]+"/"+sline[1]
        prodName = sline[5].strip()
        form = sline[2]
        strength = sline[3]
        status = 'Unknown'
        if strength.find(" **Federal ") > -1: #(" **Federal Register determination that product was not discontinued or withdrawn for safety or efficacy reasons**") > -1:
            status = 'Discontinued FR'
            strength = strength[:strength.find(" **")]
        for ingred in parseIngred(sline[6], uniiPT, uniiALL, missing):
            if ingred not in UNII2prods:
                UNII2prods[ingred] = []
            UNII2prods[ingred].append(NDA)
        prods[NDA] = [NDA, prodName, form, strength, status]
        line = fp.readline()
    fp.close()

    # write out ingredients that don't map to UNIIs
    missingUNIIsfile = maindir+"/temp/missingUNIIs-"+getTimeStamp()+".txt"
    fp = open(missingUNIIsfile, 'w')
    m2 = []

    for key in missing.keys():
        m2.append([len(missing[key]), key, missing[key][0]])
    m2.sort()
    m2.reverse()
    fp.write("Number of products\tIngredient\tExample NDA\n")

    for item in m2:
        fp.write(str(item[0])+"\t"+item[1]+"\t"+item[2]+"\n")
    fp.close()

    print("Prods in memory:", len(prods))

    # read in marketing status
    fp = io.TextIOWrapper(zfp.open('MarketingStatus.txt', 'r'), encoding='cp1252')
    markt = readTabFP(fp)
    fp.close()
    sl = dict()
    sl['1'] = 'Prescription'
    sl['2'] = 'Over-the-counter'
    sl['3'] = 'Discontinued'
    sl['4'] = 'None (Tentative Approval)'

    #MarketingStatusID	ApplNo	ProductNo
    for entry in markt['table']:
        key = entry[1]+"/"+entry[2]
        status = sl[entry[0]]
        if key in prods and prods[key][-1] != 'Discontinued FR':
            prods[key][-1] = status
        #else:
        #    print key, status

    # read in sponsor and application type
    fp = io.TextIOWrapper(zfp.open('Applications.txt', 'r'))
    appInfo = readTabFP(fp)
    fp.close()

    #ApplNo	ApplType	ApplPublicNotes	SponsorName
    #0          1               2               3
    apps = dict()
    for entry in appInfo['table']:
        apps[entry[0]] = entry

    for key in prods.keys():
        if key[0:6] in apps:
            prods[key].append(apps[key[0:6]][1])
            prods[key].append(apps[key[0:6]][3])
        else:
            prods[key].append('')
            prods[key].append('')

    # read in submission dates
    fp = io.TextIOWrapper(zfp.open('Submissions.txt', 'r'), encoding='cp1252')
    submDates = readTabFP(fp)
    fp.close()
    #ApplNo	SubmissionClassCodeID	SubmissionType	SubmissionNo	SubmissionStatus	SubmissionStatusDate	SubmissionsPublicNotes	ReviewPriority
    #0          1                       2               3               4                       5                       6                       7
    subm = dict()

    for entry in submDates['table']:
        if entry[0] not in subm:
            subm[entry[0]] = entry[5][0:10]
        elif subm[entry[0]] > entry[5][0:10]:
            subm[entry[0]] = entry[5][0:10]

    for key in prods.keys():
        if key[0:6] in subm:
            prods[key].append(subm[key[0:6]])
            prods[key].append('Drugs@FDA')
        else:
            prods[key].append('')
            prods[key].append('')

    zfp.close()

    # read in Orange Book products
    zfp = zipfile.ZipFile(obfile, 'r')
    fp = io.TextIOWrapper(zfp.open('products.txt', 'r'))
    obprods = readTabFP(fp, True, '~')
    fp.close()

    #Ingredient~DF;Route~Trade_Name~Applicant~Strength~Appl_Type~Appl_No~Product_No~TE_Code~Approval_Date~RLD~RS~Type~Applicant_Full_Name
    #0          1        2          3         4        5         6       7          8       9             10  11 12   13
    sponsors = dict()
    appTypes = dict()
    appTypes['N'] = 'NDA'
    appTypes['A'] = 'ANDA'
    sl['RX'] = 'Prescription'
    sl['OTC'] = 'Over-the-counter'
    sl['DISCN'] = 'Discontinued'

    for entry in obprods['table']:
        appl = entry[6]+'/'+entry[7]
        # other product info is updated
        ts = time.strptime("Jan 1, 1982", "%b %d, %Y")
        if entry[9] != "Approved Prior to Jan 1, 1982":
            ts = time.strptime(entry[9], "%b %d, %Y")
        date = time.strftime("%Y-%m-%d", ts)
        status = sl[entry[12]]
        if entry[4].find(" **Federal ") > -1: # (" **Federal Register determination that product was not discontinued or withdrawn for safety or efficacy reasons**") > -1:
            status = 'Discontinued FR'
            entry[4] = entry[4][:entry[4].find(" **")]
        sponsors[entry[3].strip()] = entry[13]

        # verify ingredients are mapped to this product
        for ingred in parseIngred(entry[0], uniiPT, uniiALL, missing):
            if appl not in prods:
                prods[appl] = [appl, entry[2], entry[1], entry[4], status, appTypes[entry[5]], entry[3], date, 'OrangeBook']   #['072437/001', 'FENOPROFEN CALCIUM', 'CAPSULE;ORAL', 'EQ 200MG BASE', 'Discontinued', 'ANDA', 'PAR PHARM', '1988-08-22', 'Drugs@FDA']
                print("added orange book prod:", prods[appl])
            if ingred not in UNII2prods:
                UNII2prods[ingred] = []
                #raise ValueError('Ingredient from Orange Book not found in products:'+ingred)
            if appl not in UNII2prods[ingred]:
                UNII2prods[ingred].append(appl)
                #raise ValueError('Product number from Orange Book unexpectedly mapped to unii:'+appl+":"+ingred)

        if appl in prods:
            if prods[appl][4] != status and status == 'Discontinued FR':
                prods[appl][4] = status
            if prods[appl][5] == '':
                prods[appl][5] = appTypes[entry[5]]
            if prods[appl][6] == '':
                prods[appl][6] = entry[3].strip()
            if prods[appl][7] == '' or prods[appl][7] > date:
                prods[appl][7] = date
                prods[appl][8] = 'OrangeBook'
        else:
            print(appl)
            print(entry)
            raise ValueError('Product in Orange Book not found in products:'+appl)

    #prods has format: key="NDA/prodno" : [0:NDA/prodno, 1:Prod name, 2:Form;Doseage, 3:Strength, 4:Availability (Prescription; Over-the-counter; Discontinued), 5:Appl type (NDA), 6:Sponsor, 7:Marketing startDate, 8: Marketing startDate ref]

    # read in NME prod dates file
    #Year	Trademark	Generic Name	UNII	Date [mm-dd-yy]	App Type	NDA/BLA No.	Sponsor	Date Ref
    #0          1               2               3       4               5               6               7       8
    fdaNMEs = readTabFile(fdaNMEfile)
    fdaNMEdates = dict()

    for entry in fdaNMEs['table']:
        if len(entry) < 5:
            sys.stderr.write("Bad entry:"+str(entry)+"\n")
        unii = entry[3]
        year = entry[0]
        date = year+"-12-31"
        if entry[4] != '':
            ts = time.strptime(entry[4], "%m-%d-%y")
            date = time.strftime("%Y-%m-%d", ts)
            if date[0:4] != year:
                date = year + date[4:]
        dateRef = entry[8]# .decode('latin-1').encode('ascii', 'replace')
        product = entry[1]
        sponsor = entry[7]
        appNo = entry[6]
        if appNo == '':
            appNo = product+" ("+year+") " + sponsor
        else:
            appNo = appNo[:len(appNo)-4]+appNo[-3:]
            while len(appNo) < 6:
                appNo = "0" + appNo
            fdaNMEdates[appNo] = [date, dateRef]
            appNo =  appNo + " " + product+" ("+year+") " + sponsor
        appType = entry[5]
        prod = [appNo, product, '', '', '', appType, sponsor, date, dateRef]
        prods[appNo] = prod
        if unii not in UNII2prods:
            UNII2prods[unii] = []
        UNII2prods[unii].append(appNo)
    for prod in prods.keys():
        if prod[0:6] in fdaNMEdates:
            nmeDate = fdaNMEdates[prod[0:6]]
            date = prods[prod][-2]
            method = prods[prod][-1]
            if date == '' or (date == '1982-01-01' and method == 'OrangeBook'):
                prods[prod][-2] = nmeDate[0]
                prods[prod][-1] = nmeDate[1]
            elif nmeDate[0] < date: # we should go back and curate these!
                #print("different dates:",date, nmeDate, prods[prod])
                # curated 5/18/2021 with 1e7b841d
                prods[prod][-2] = nmeDate[0]
                prods[prod][-1] = nmeDate[1]

    # get active moieties from tyler's dump file
    activeMoiety = dict()
    with gzip.open( gsrsDumpfile, 'rb') as fp:
        line = fp.readline()
        while line != b'':
            r = json.loads(str(line, errors='ignore'))
            if 'approvalID' in r: #alternative definitions don't have approvalIDs
                addrel = 0
                for rel in r['relationships']:
                    if rel['type'] == 'ACTIVE MOIETY':
                        addrel = 1
                        if rel['relatedSubstance']['approvalID'] not in activeMoiety:
                            activeMoiety[rel['relatedSubstance']['approvalID']] = []
                        activeMoiety[rel['relatedSubstance']['approvalID']].append(r['approvalID'])
                if addrel == 0:
                    if r['approvalID'] not in activeMoiety:
                        activeMoiety[r['approvalID']] = []
                    activeMoiety[r['approvalID']].append(r['approvalID'])
            line = fp.readline()
    print("read unii dump file")

    # write out new approval years file
    outfile =  maindir+"/data/approvalYears-"+getTimeStamp()+".txt"
    fp = open(outfile, 'w')
    header = "UNII\tApproval_Year\tDate\tDate_Method\tApp_Type\tApp_No\tSponsor\tProduct\tUrl\tactive\tComment\n"
    fp.write(header)

    for unii in activeMoiety.keys():
        early = dict()
        earlyDate = getTimeStamp()

        for otherunii in activeMoiety[unii]:
            if otherunii in UNII2prods:
                for prod in UNII2prods[otherunii]:
                    entry = prods[prod]

                    akey = entry[0][0:6]
                    if akey not in early:
                        early[akey] = entry
                    else: # merge records
                        if early[akey][-2] == '' or (entry[-2] != '' and early[akey][-2] > entry[-2]):
                            early[akey] = entry

                    if entry[-2] != '' and entry[-2] < earlyDate and entry[-2] > "1938-08-01":
                        earlyDate = entry[-2]

                    # later products might still be marketed, copy status into early record, e.g. I-131 021305 HICON
                    if early[akey][-5] != "Prescription" and early[akey][-5] != "Over-the-counter" and (entry[-5] == "Prescription" or entry[-5] == "Over-the-counter"):
                        early[akey][-5] = entry[-5]


        for key in early.keys():
            myunii = unii
            for otherunii in activeMoiety[unii]:
                if otherunii in UNII2prods:
                    for prod in UNII2prods[otherunii]:
                        akey = prods[prod][0][0:6]
                        if akey == key:
                            myunii = otherunii

            if early[key][-2] != '' and early[key][-2][0:4] == earlyDate[0:4] and early[key][-2] > earlyDate:
                early[key][-2] = earlyDate
            writeInitApp(fp, unii, early[key], early[key][-2], myunii)

    # write out additional CBER BLAs
    writeCBERBLAs(purpleBookfile, fdaSPLfile, fp, uniiPT, uniiALL)
            
    # write out all products data
    prod2UNIIs = dict()
    for unii in UNII2prods.keys():
        for prod in UNII2prods[unii]:
            if prod not in prod2UNIIs:
                prod2UNIIs[prod] = []
            prod2UNIIs[prod].append(unii)

    UNII2pt = dict()

    for key in resolverCache.keys():
        UNII2pt[resolverCache[key]] = key

    for key in uniiPT.keys():
        UNII2pt[uniiPT[key]] = key

    productsfile = maindir+"/temp/products-"+getTimeStamp()+".txt"
    fp = open(productsfile, 'w')

    header = 'NDA\tProduct\tForm;Route\tStrength\tStatus\tAppl Type\tSponsor\tDate\tDate Ref\tUNIIs\tIngredients\n'
    fp.write(header)
    
    for prod in prods.keys():
        uniis = []
        if prod in prod2UNIIs: # ingreds that don't have uniis
            uniis = prod2UNIIs[prod]
        uniilist = '; '.join(uniis)
        ingreds = []
        for unii in uniis:
            if unii not in UNII2pt:
                print(unii, uniilist, prod)
                sys.exit()
            ingreds.append(UNII2pt[unii])
        ingredlist = '; '.join(ingreds)
        entry = prods[prod]
        entry.append(uniilist)
        entry.append(ingredlist)
        outline = ""
        for item in entry:
            outline = outline + item + '\t'
        fp.write(outline+'\n')
    fp.close()
