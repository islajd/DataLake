const { Workbook } = require("exceljs")
const AWS = require("aws-sdk")
const firehose = new AWS.Firehose({
    region: "eu-central-1"
})
const { translateArray } = require("./utils")

const readExcelFile = async (key) => {
    try {
        const workbook = new Workbook()
        const workbookReader = await workbook.xlsx.readFile(key)
        const rows = []

        const firstSheet = workbookReader.worksheets[0]
        if(!firstSheet)
            throw new Error("Empty File.")

        firstSheet.eachRow((row) => {
            const rowValues = []
            row.eachCell({includeEmpty: true}, (cell) => {
                const cellValue = cell.text ? cell.text.trim() : null
                rowValues.push(cellValue)
            })
            rows.push(rowValues)
        })

        const jsonRows = []
        const [headersRow, ...restRows] = rows
        for(const row of restRows) {
            const obj = {}
            for(let i = 0; i < headersRow.length; i++)
                if(headersRow[i])
                    obj[headersRow[i]] = row[i] || null
            jsonRows.push(obj)
        }        

        return jsonRows      
    } catch (error) {
        console.log("🚀 ~ file: index.js ~ line 35 ~ readExcelFile ~ error", error)
    }
}

const handler = async () => {
    let rows = await readExcelFile("OnlineRetail.xlsx")
    rows = translateArray(rows)

    for(let row of rows){
        console.log("🚀 ~ file: index.js ~ line 48 ~ handler ~ row", JSON.stringify(row))
        let putParams = {
            DeliveryStreamName: "firehose",
            Record: {
                Data: Buffer.from(JSON.stringify(row))
            }
        }
        let firehoseResponse = await firehose.putRecord(putParams).promise()
        console.log("🚀 ~ file: index.js ~ line 54 ~ handler ~ firehoseResponse", firehoseResponse)

        
    }

}

handler()
