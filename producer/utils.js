const { default: Big } = require("big.js")

const translateArray = array => array.map((item) => {
    return {
        ...item,
        datehour: +Big(new Date(item.InvoiceDate).getTime()).div(1000).toFixed(0)
    }
})

module.exports = {
    translateArray
}