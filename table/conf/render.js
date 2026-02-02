renderFunctions.renderOtherInfo = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    try {
      columnString = columnString.replace(/'/g, '"')
      const parsed = JSON.parse(columnString)
      if (Array.isArray(parsed)) {
        columnString = parsed
      }
    } catch (e) {
      return columnString
    }
    const links = []
    for (let i = 0; i < columnString.length; i++) {
      const url = columnString[i]['@URL']
      const type = columnString[i]['@type'] || 'not specified'
      const title = columnString[i]['@title'] + ` (type: ${type})`
      const attrs = `href="${url}" title="${title}"`
      links.push(`<a ${attrs} target="_blank">${url}</a>`)
    }
    return links.join('<br>')
  }
}

renderFunctions.renderDatasetID = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }

    const columnNames = config.dataTables.columns.map(c => c.name)

    // TODO: Not all have "_v01".
    const base = 'https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/'
    const fnameAll = "https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml"
    const fnameCDF = base + '0MASTERS/' + columnString.toLowerCase() + '_00000000_v01.cdf'
    const fnameJSON = base + '0JSONS/' + columnString.toLowerCase() + '_00000000_v01.json'
    const fnameSKT = base + '0SKELTABLES/' + columnString.toLowerCase() + '_00000000_v01.skt'
    const fnameHAPI1 = `https://cdaweb.gsfc.nasa.gov/hapi/info?id=${columnString}`
    const fnameHAPI2 = `https://cottagesystems.com/server/cdaweb/hapi/info?id=${columnString}`

    columnString = `${columnString}`
    columnString += '<br><span style="font-size:0.75em">'
    columnString += ` <a href="${fnameAll}"   title="all.xml" target="_blank">A</a>`
    //columnString += ` <a href="${fnameCDF}"   title="Master CDF" target="_blank">M</a>`
    columnString += ` <a href="${fnameJSON}"  title="Master JSON" target="_blank">M</a>`
    //columnString += ` <a href="${fnameSKT}"   title="Master Skeleton Table" target="_blank">SK</a>`

    const tableName = config.dataTablesAdditions.tableMetadata.tableName

    if (tableName === 'cdaweb.dataset') {
      const index = columnNames.indexOf('spase_DatasetResourceID')
      const fnameSPASE = row[index].replace('spase://', 'https://spase-metadata.org/') + '.json'
      columnString += `&nbsp;<a href="${fnameSPASE}" title="SPASE">S</a>`
      const link = '../variable/#datasetID=' + row[0]
      columnString += ` <a href="${link}" title="Parameter table" target="_blank"><i>P</i></a>`
    }

    if (tableName === 'cdaweb.variable') {
      const link = '../dataset/#datasetID=' + row[0]
      columnString += ` <a href="${link}" title="Dataset table" target="_blank"><i>D</i></a>`

      const index = columnNames.indexOf('VAR_TYPE')
      if (row[index] === 'data') {
        columnString += ` <a href="${fnameHAPI1}" title="HAPI Info" target="_blank">H<sub>1</sub></a>`
        columnString += ` <a href="${fnameHAPI2}" title="HAPI Info Dev Server" target="_blank">H<sub>2</sub></a>`
      }
    }

    if (tableName === 'hapi.dataset') {
      const index = columnNames.indexOf('additionalMetadata/contentURL')
      columnString += ` <a href="${row[index]}" title="SPASE" target="_blank">S</a>`
      const link = '../parameter/#id=' + row[0]
      columnString += ` <a href="${link}" title="Parameters table" target="_blank"><i>P</i></a>`
    }

    if (tableName === 'hapi.parameter') {
      const link = '../dataset/#id=' + row[0]
      columnString += ` <a href="${link}" title="Dataset table" target="_blank"><i>D</i></a>`
    }

    if (tableName === 'spase.dataset') {
      console.log(row[1].replace('spase://', 'https://spase-metadata.org/'))
      const fnameSPASE = row[1].replace('spase://', 'https://spase-metadata.org/') + '.json'
      columnString += ` <a href="${fnameSPASE}" title="SPASE" target="_blank">S</a>`
      const link = '../parameter/#datasetID=' + row[0]
      columnString += ` <a href="${link}" title="Parameters table" target="_blank"><i>P</i></a>`
    }

    if (tableName === 'spase.parameter') {
      const link = '../dataset/#datasetID=' + row[0]
      columnString += ` <a href="${link}" title="Dataset table" target="_blank"><i>D</i></a>`
    }

    columnString += '</span>'

    return columnString
  }
}
