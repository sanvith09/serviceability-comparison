const axios = require("axios");
const fs = require("fs");
const csv = require("csv-parser");

function normalizeString(str) {
  return str ? str.trim().toLowerCase() : "";
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function toCanonicalNaming(str, component) {

  if (!str || typeof str !== "string") {
    console.error("Invalid string:", str);
    return "UNKNOWN";
  }

  const lowerStr = str.toLowerCase().trim();

  const mappings = {
    type: {
      "delivery": "DELIVERY",
      "collect": "COLLECT",
      "pickup": "COLLECT",
      "pickupcopec": "COLLECT",
    },
    subtype: {
      "home": "HOME",
      "in_store": "IN_STORE",
      "store": "IN_STORE",
      "copec": "COPEC",
    },
    method: {
      "scheduled": "SCHEDULED",
      "daterange": "DATE_RANGE",
      "sameday": "SAME_DAY",
      "24hours": "NEXT_DAY",
      "nextday": "NEXT_DAY",
      "pickup": "SCHEDULED",
      "pickupcopec": "SCHEDULED",
    },
    combined: {
      "pickup": "COLLECT-IN_STORE-SCHEDULED",
      "pickupcopec": "COLLECT-COPEC-SCHEDULED",
      "scheduled": "DELIVERY-HOME-SCHEDULED",
      "sameday": "DELIVERY-HOME-SAME_DAY",
      "24hours": "DELIVERY-HOME-NEXT_DAY",
      "daterange": "DELIVERY-HOME-DATE_RANGE",
    }
  };

  if (component && mappings[component]) {
    return mappings[component][lowerStr] || str.toUpperCase();
  } else {
    const combined = mappings.combined[lowerStr];
    if (combined) return combined;

    const typeMap = mappings.type[lowerStr];
    if (typeMap) return typeMap;

    const subtypeMap = mappings.subtype[lowerStr];
    if (subtypeMap) return subtypeMap;

    const methodMap = mappings.method[lowerStr];
    if (methodMap) return methodMap;

    return str.toUpperCase();
  }
}

function filterServiceableEomOptions(eomOptions) {
  return eomOptions
    .filter(option => option.status?.code === 2000) 
    .map(option => ({
      type: toCanonicalNaming(option.type),
      stockLevel: option.status.description === "OK" ? "IN_STOCK" : "LIMITED_STOCK", 
      facility: option.facility || "N/A", 
      deliveryType: option.deliveryType || "N/A", 
    }));
}

function filterServiceableGivOptions(givOptions, givCount) {
  return givOptions
    .filter(option => option.serviceableStockLevel === "IN_STOCK" || option.serviceableStockLevel === "LIMITED_STOCK")
    .slice(0, givCount)
    .map(option => {
      const shippingType = toCanonicalNaming(option.shippingType, 'type');
      const shippingSubType = toCanonicalNaming(option.shippingSubType, 'subtype');
      const shippingMethod = toCanonicalNaming(option.shippingMethod, 'method');
  
      const isFullMethod = shippingMethod.startsWith(`${shippingType}-${shippingSubType}`);
      
      return {
        type: shippingMethod,
        stockLevel: option.serviceableStockLevel,
        facility: option.serviceNodeCode || "N/A",
        deliveryType: isFullMethod 
          ? shippingMethod
          : `${shippingType}-${shippingSubType}-${shippingMethod}`,
      };
    });
}

async function fetchPdpShippingOptions(itemID, comunaCode, startDate) {
  try {
    const formattedDate = startDate.split("/").reverse().join("-");
    if (!formattedDate) {
      console.error("Invalid startDate, cannot proceed with request.");
      return [];
    }

    const response = await axios.post(
      "https://ccom-shipping-dates-pdp-service.ecomm.cencosud.com/shipping-dates-pdp/v1/shipping-dates",
      {
        source: "Paris.cl",
        comunaCode: comunaCode,
        startDate: formattedDate,
        numberOfDates: 5,
        grouping: true,
        filters: {
          shippingMethod: [
            "sameday",
            "scheduled",
            "pickup",
            "pickupCopec",
            "daterange",
            "24hours",
          ],
          onlySmallInStoresNotParis: true,
        },
        location: {
          latitude: -33.4513,
          longitude: -70.6653,
          address: "Grajales 2121-2149, Santiago, Región Metropolitana",
        },
        items: [
          {
            itemName: itemID,
            externalId: "4321fcc2e1cd2f70ad2234f61a",
            quantity: 1,
            marketPlace: false,
            size: "small",
            filters: {
              sameDay: true,
              marketPlace: false,
              originType: 0,
            },
          },
        ],
        jornadas: ["TH"],
      },
      {
        headers: {
          "Content-Type": "application/json",
          apikey: "5obvH3J0QjxS7HxTLeE66VMp37HFWhYnCnl9Wik2DfmOL",
        },
      }
    );
    return response.data[0]?.shippingOptions || [];
  } catch (error) {
    console.error("Error fetching PDP shipping options:", error.message);
    return [];
  }
}
async function fetchServiceabilityShippingOptions(itemID, locality) {
  try {
    const response = await axios.get(
      `https://be-paris-backend-cl-ms-api.ecomm.cencosud.com/products/${itemID}/serviceability?locality=${locality}`,
      {
        headers: {
          Cookie: "eyJraWQiOiJ1cy1lYXN0LTEtNiIsInR5cCI6IkpXVCIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJ1cy1lYXN0LTE6MWIzYWE1MDQtZGIxOS1jNjEwLTliZTItYWNjN2E2NDU1NDk2IiwiYXVkIjoidXMtZWFzdC0xOjQzNzdmYTNlLWViOTctNDAxNi1iM2FlLWEzMmFkOTZiMTliYyIsImFtciI6WyJ1bmF1dGhlbnRpY2F0ZWQiXSwiaXNzIjoiaHR0cHM6Ly9jb2duaXRvLWlkZW50aXR5LmFtYXpvbmF3cy5jb20iLCJleHAiOjE3Mjk4MDA1NTQsImlhdCI6MTcyOTc5OTk1NH0.b1U5bTInDE9b99JXVAySdUE7uZAdw3toYci44lM_wc6uNoq1V2vhmzRHACXVQTFjsCEeURCljMT9JfiWsrQvF7vSczGplcUWrqlDWzji3c8x7HMp8NmDgQcAIFdNzTxUaMlj4Yg3HRo7vrsb5_ei7JUDTLeSOfNHXIZSoiL8ATCtp7iyDo_ZXeD1GhTCmgvZZmaTormWaBmNo2XWYgZbWy1yrKZhkO4Tl0HZUQZ4Du-BpexaBJgEbT5MYiXzuQJ8SBylKUWUoCmK4wjDoTsO_cLw6hwWFGrNJALHhwCTK5pkrpo4febva3Csl6RCgoXYc9_4FtzlMJMXZk6I9JoYEg",
        },
      }
    );
    return response.data.shippingOptions || [];
  } catch (error) {
    console.error("Error fetching serviceability shipping options:", error.message);
    return [];
  }
}

function areShippingOptionsIdentical(eomOptions, givOptions) {
  const eomCanonical = eomOptions.map(option => ({
    type: toCanonicalNaming(option.type),
    stockLevel: option.stockLevel,
    facility: option.facility,
    deliveryType: option.deliveryType,
  }));

  const givCanonical = givOptions.map(option => ({
    type: toCanonicalNaming(option.type),
    stockLevel: option.stockLevel,
    facility: option.facility,
    deliveryType: option.deliveryType,
  }));

  if (eomCanonical.length === givCanonical.length) {
    console.log("EOM and GIV counts are equal");
    return true;
  }

  let matched = true;

  eomCanonical.forEach(eomOption => {
    const matchedOption = givCanonical.find(
      givOption =>
        eomOption.type === givOption.type && 
        eomOption.stockLevel === givOption.stockLevel &&
        eomOption.facility === givOption.facility &&
        eomOption.deliveryType === givOption.deliveryType
    );

    if (!matchedOption) {
      console.log("Unmatched EOM option:", eomOption);
      matched = false;
    }
  });

  givCanonical.forEach(givOption => {
    const matchedOption = eomCanonical.find(
      eomOption =>
        eomOption.type === givOption.type && 
        eomOption.stockLevel === givOption.stockLevel &&
        eomOption.facility === givOption.facility &&
        eomOption.deliveryType === givOption.deliveryType
    );

    if (!matchedOption) {
      console.log("Unmatched GIV option:", givOption);
      matched = false;
    }
  });

  return matched;
}

async function processRow(itemID, comunaCode, locality, startDate) {
  try {
    const eomShippingOptions = await fetchPdpShippingOptions(itemID, comunaCode, startDate);
    const givShippingOptions = await fetchServiceabilityShippingOptions(itemID, locality);

    const filteredEomOptions = filterServiceableEomOptions(eomShippingOptions);

    const filteredGivOptions = filterServiceableGivOptions(givShippingOptions, givShippingOptions.length);

    const eomArray = filteredEomOptions.map(option => option.type);
    const givArray = filteredGivOptions.map(option => option.deliveryType);

    const presentInEomNotGiv = eomArray.filter(opt => !givArray.includes(opt));
    const presentInGivNotEom = givArray.filter(opt => !eomArray.includes(opt));

    const presentEomNotGivStr = presentInEomNotGiv.length > 0 ? presentInEomNotGiv.join(" | ") : "0";
    const presentGivNotEomStr = presentInGivNotEom.length > 0 ? presentInGivNotEom.join(" | ") : "0";

    console.log(`EOM Options Count: ${eomArray.length}, GIV Options Count: ${givArray.length}`);
    const isIdentical = areShippingOptionsIdentical(filteredEomOptions, filteredGivOptions);

    const status = isIdentical ? "identical" : "differ";

    let serviceable = "N/A";
    if (eomArray.length > 0 && givArray.length === 0) {
      serviceable = "SERVICEABLE IN EOM but NOT in GIV";
    } else if (eomArray.length === 0 && givArray.length > 0) {
      serviceable = "SERVICEABLE IN GIV but NOT in EOM";
    } else if (eomArray.length > 0 && givArray.length > 0) {
      serviceable = "SERVICEABLE IN BOTH";
    }

    const facility = eomArray.length > 0 ? filteredEomOptions[0].facility : "N/A";
    const deliveryType = eomArray.length > 0 ? filteredEomOptions[0].deliveryType : "N/A";

    const availableEomOptions = eomArray.join(" | ");
    const availableGivOptions = givArray.join(" | ");

    return {
      itemID,
      comunaCode,
      locality,
      startDate,
      status,
      eomCount: eomArray.length,
      givCount: givArray.length,
      serviceable,
      facility,
      deliveryType,
      availableEomOptions,
      availableGivOptions,
      PresentinEomnotGiv: presentEomNotGivStr,
      PresentinGivnotEom: presentGivNotEomStr,
    };
  } catch (error) {
    console.error("Error processing row:", error.message);
    return {
      itemID,
      comunaCode,
      locality,
      startDate,
      status: "error",
      eomCount: 0,
      givCount: 0,
      serviceable: "N/A",
      facility: "N/A",
      deliveryType: "N/A",
      availableEomOptions: "N/A",
      availableGivOptions: "N/A",
      PresentinEomnotGiv: "0",
      PresentinGivnotEom: "0",
    };
  }
}

async function main() {
  const rows = [];
  const results = [];

  fs.createReadStream("input_data.csv")
    .pipe(csv())
    .on("data", row => {
      if (row.itemID && row.comunaCode && row.locality && row.date) {
        rows.push(row);
      } else {
        console.log("Skipping row with missing data:", row);
      }
    })
    .on("end", async () => {
      for (let i = 0; i < rows.length; i += 100) {
        const chunk = rows.slice(i, i + 100);

        console.log(`Processing SKUs ${i + 1} to ${Math.min(i + 100, rows.length)}...`);

        const chunkResults = await Promise.all(
          chunk.map(row => processRow(row.itemID, row.comunaCode, row.locality, row.date))
        );

        results.push(...chunkResults);

        console.table(chunkResults);

        if (i + 100 < rows.length) {
          console.log("Waiting 5 seconds before processing the next batch...");
          await sleep(5000);
        }
      }

      console.log("All SKUs processed. Writing results to output file...");

      const outputStream = fs.createWriteStream("output_results.csv");
      outputStream.write(
        "itemID,comunaCode,locality,startDate,status,eomCount,givCount,serviceable,facility,deliveryType,availableEomOptions,availableGivOptions,PresentinEomnotGiv,PresentinGivnotEom\n"
      );
      results.forEach(result => {
        outputStream.write(
          `${result.itemID},${result.comunaCode},${result.locality},${result.startDate},${result.status},${result.eomCount},${result.givCount},${result.serviceable},${result.facility},${result.deliveryType},${result.availableEomOptions},${result.availableGivOptions},${result.PresentinEomnotGiv},${result.PresentinGivnotEom}\n`
        );
      });
      outputStream.end();

      console.log("Results written to output_results.csv");
    });
}

main();
