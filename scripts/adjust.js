#!/usr/bin/env node
'use strict'

const { createReadStream } = require('fs')
const { pipeline } = require('stream')

const { parse: createCsvParser } = require('@fast-csv/parse')
const { pipe } = require('ramda')

const { lowerCaseMappings } = require('./mappings-lower-case')
const { upperCaseMappings } = require('./mappings-upper-case')
const { headerCsv, headerOrderIn, headerOrderOut } = require('./mappings-header')

function exit (code = 0, message = null) {
  if (message && code === 0) console.log(message)
  else if (message && code !== 0) console.error(message)
  process.exit(code)
}

// Do NOT assign to process.stdout.write, won't work
const print = s => process.stdout.write(s)

const normalizeCaseName = str => str
  .split(' ')
  .map(word => {
    if (lowerCaseMappings.includes(word)) return word.toLowerCase()
    else if (upperCaseMappings.includes(word)) return word.toUpperCase()
    else return word[0] + word.slice(1).toLowerCase()
  })
  .join(' ')

const normalizeCaseSentence = str => str
  .split(' ')
  .map((word, n) => {
    if (!word) return ''
    else if (upperCaseMappings.includes(word)) return word.toUpperCase()
    else if (n === 0) return word[0] + word.slice(1).toLowerCase()
    else return word.toLowerCase()
  })
  .join(' ')

function normalize (input, field) {
  const normalizers = {
    'city': normalizeCaseName,
    'comments': normalizeCaseSentence,
    'country': normalizeCaseName,
    'createdAt': normalizeCaseName,
    'name': normalizeCaseName,
    'type': val => val = 'O' ? 'OPERATING' : 'SEGMENT',
    'updatedAt': normalizeCaseName,
    'website': val => val
      ? val.startsWith('HTTP') ? val.toLowerCase() : 'https://' + val.toLowerCase()
      : val
  }
  return normalizers[field] ? normalizers[field](input) : input
}

function createProcessor (format) {
  function processItemJson (exchange) {
    let newExchange = Object.create(null)
    headerOrderOut.map(field => {
      newExchange[field] = normalize(exchange[field], field)
    })
    return JSON.stringify(newExchange)
  }

  function processItemCsv (exchange) {
    let first = true
    let row = ''
    let newExchange = new Map()
    headerOrderOut.map(field => newExchange.set(field, normalize(exchange[field], field)))
    newExchange.forEach(val => {
      val = val.replace(/\"/g, "''")
      if (first) {
        first = false
        row += `"${val}"`
      } else row += `,"${val}"`
    })
    return row + ''
  }

  if (format === 'json') return processItemJson
  else if (format === 'csv') return processItemCsv
}

function createPrinter (format) {
  let firstChunk = true
  return (chunk) => {
    if (firstChunk) {
      print(chunk + (format === 'csv' ? '\n' : ''))
      firstChunk = false
    } else {
      print((format === 'json' ? ',' : '') + chunk + (format === 'csv' ? '\n' : ''))
    }
  }
}

function finalize (format) {
  return err => {
    if (err) exit(1, err)
    else print(format === 'json' ? ']\n' : '\n')
  }
}

function adjust (filename, format = 'json') {
  if (format !== 'json' && format !== 'csv') exit(1, 'Invalid format')
  const fReader = createReadStream(filename)
  const csvParser = createCsvParser({
    headers: headerOrderIn,
    renameHeaders: true,
  })
    .on('data', pipe(createProcessor(format), createPrinter(format)))

  if (format === 'json') print('[')
  else if (format === 'csv') {
    headerOrderOut.map((h, n) => print(`${n !== 0 ? ',' : ''}"${h}"`))
    print('\n')
  }

  pipeline(fReader, csvParser, finalize(format))
}

adjust(process.argv[2], process.argv[3])
