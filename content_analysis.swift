#!/usr/bin/swift

import Foundation

class Graph {
	
	class Node {
		let id: Int
		var favoriteCount: Int
		var retweetCount: Int
		var wordCount: Int
		var tweetIds: Set<Int>
		var category: Category
		
		init(id: Int, favoriteCount: Int, retweetCount: Int, wordCount: Int, tweetIds: Set<Int>, category: Category) {
			self.id = id
			self.favoriteCount = favoriteCount
			self.retweetCount = retweetCount
			self.wordCount = wordCount
			self.tweetIds = tweetIds
			self.category = category
		}
	}
	
	struct Edge: Hashable {
		let sourceNodeId: Int
		let targetNodeId: Int
	}
	
	enum Category: CustomStringConvertible, Equatable {

		case uncategorized
		case garbage
		case other(string: String)
		
		var description: String {
			switch self {
			case .uncategorized:
				return "uncategorized"
			case .garbage:
				return "garbage"
			case .other(let string):
				return string
			}
		}
		
		var isDiscardable: Bool {
			switch self {
			case .uncategorized, .garbage:
				return true
			case .other:
				return false
			}
		}
		
		static func from(string value: String?) -> Category {
			guard let value = value else {
				return .uncategorized
			}
			
			if value == "uncategorized" {
				return .uncategorized
			} else if value == "garbage" {
				return .garbage
			} else {
				return .other(string: value)
			}
		}
	}
	
	private var nodes: [String: Node] = [:]
	private var uncategorizedNodes: [String: Node] = [:]
	private var edgeWeights: [Edge: Int] = [:]
	
	private var tweetNodeIds: [Int: Set<Int>] = [:]
	
	private var categorizedWords: [String: String] = [:]
	
	private var totalCategorizedWordCount = 0
	private var totalWordCount = 0
	
	init(categorizedWords: [String: String]) {
		self.categorizedWords = categorizedWords
	}
	
	func addNodesAndEdges(fromRow row: [String: String]) {
		guard
			let word = row["word"],
			let favoriteCountString = row["favoriteCount"],
			let favoriteCount = Int(favoriteCountString),
			let retweetCountString = row["retweetCount"],
			let retweetCount = Int(retweetCountString),
			let tweetIdString = row["id"],
			let tweetId = Int(tweetIdString),
			nodes[word]?.tweetIds.contains(tweetId) != true else {
				return
		}
		
		let node: Node
		
		if let existingNode = nodes[word] ?? uncategorizedNodes[word] {
			node = existingNode
			node.favoriteCount += favoriteCount
			node.retweetCount += retweetCount
			node.wordCount += 1
			node.tweetIds.insert(tweetId)
		} else {
			node = Node(id: nodes.count,
						favoriteCount: favoriteCount,
						retweetCount: retweetCount,
						wordCount: 1,
						tweetIds: [tweetId],
						category: Category.from(string: categorizedWords[word]))
			
			if !node.category.isDiscardable {
				nodes[word] = node
			} else if node.category == .uncategorized {
				uncategorizedNodes[word] = node
			}
		}
		
		totalWordCount += 1
		
		guard !node.category.isDiscardable else {
			return
		}
		
		if let nodeIds = tweetNodeIds[tweetId] {
			for previousNodeId in nodeIds {
				let edge = Edge(sourceNodeId: node.id, targetNodeId: previousNodeId)
				edgeWeights[edge] = (edgeWeights[edge] ?? 0) + 1
			}
			tweetNodeIds[tweetId]?.insert(node.id)
		} else {
			tweetNodeIds[tweetId] = [node.id]
		}
		
		totalCategorizedWordCount += 1
	}
	
	func nodesCsv() -> String {
		
		var csv = "Id,Label,favoriteRate,retweetRate,wordCount,wordFrequency,popularity,polemicity,category\n"
		
		for (word, node) in nodes {
			let favoriteRate = Double(node.favoriteCount) / Double(node.tweetIds.count)
			let retweetRate = Double(node.retweetCount) / Double(node.tweetIds.count)
			let wordFrequency = Double(node.wordCount) / Double(totalCategorizedWordCount)
			let popularity = retweetRate / wordFrequency
			let polemicity = favoriteRate == 0 ? 0.0 : (retweetRate / favoriteRate)
			
			csv += "\(node.id),\(word),\(favoriteRate),\(retweetRate),\(node.wordCount),\(wordFrequency),\(popularity),\(polemicity),\(node.category)\n"
		}
		
		return csv
	}
	
	func edgesCsv() -> String {
		var csv = "Source,Target,Type,Id,Weight\n"
		for (index, element) in edgeWeights.enumerated() {
			csv += "\(element.key.sourceNodeId),\(element.key.targetNodeId),Undirected,\(index),\(Double(element.value))\n"
		}
		return csv
	}
	
	func categorizedWordsCsv() -> String {
		
		var uncategorizedWords: [(word: String, relevance: Double)] = []
		
		for (word, node) in uncategorizedNodes {
			let favoriteRate = Double(node.favoriteCount) / Double(node.tweetIds.count)
			let retweetRate = Double(node.retweetCount) / Double(node.tweetIds.count)
			let wordFrequency = Double(node.wordCount) / Double(totalWordCount)
			let relevance = (favoriteRate * 0.000048 + retweetRate * 0.00045 + wordFrequency * 1000) / 3
			
			uncategorizedWords.append((word: word, relevance: relevance))
		}
		
		var csv = "word,category\n"
		for (word, category) in categorizedWords {
			csv += "\(word),\(category)\n"
		}
		for (word, _) in uncategorizedWords.sorted(by: { $0.relevance > $1.relevance }) {
			csv += "\(word),uncategorized\n"
		}
		return csv
	}
}

class FileStreamReader {
	
	private let encoding: String.Encoding
	private let chunkSize: Int
	private let fileHandle: FileHandle
	private var buffer: Data
	private let delimPattern : Data
	private var isAtEOF: Bool = false
	
	init?(url: URL, delimeter: String = "\n", encoding: String.Encoding = .utf8, chunkSize: Int = 4096)
	{
		guard let fileHandle = try? FileHandle(forReadingFrom: url) else { return nil }
		self.fileHandle = fileHandle
		self.chunkSize = chunkSize
		self.encoding = encoding
		buffer = Data(capacity: chunkSize)
		delimPattern = delimeter.data(using: .utf8)!
	}
	
	deinit {
		fileHandle.closeFile()
	}
	
	func rewind() {
		fileHandle.seek(toFileOffset: 0)
		buffer.removeAll(keepingCapacity: true)
		isAtEOF = false
	}
	
	func nextLine() -> String? {
		if isAtEOF { return nil }
		
		repeat {
			if let range = buffer.range(of: delimPattern, options: [], in: buffer.startIndex..<buffer.endIndex) {
				let subData = buffer.subdata(in: buffer.startIndex..<range.lowerBound)
				let line = String(data: subData, encoding: encoding)
				buffer.replaceSubrange(buffer.startIndex..<range.upperBound, with: [])
				return line
			} else {
				let tempData = fileHandle.readData(ofLength: chunkSize)
				if tempData.count == 0 {
					isAtEOF = true
					return (buffer.count > 0) ? String(data: buffer, encoding: encoding) : nil
				}
				buffer.append(tempData)
			}
		} while true
	}
}

extension String {
	func csvComponents() -> [String] {
		return self.components(separatedBy: ",").map { $0.trimmingCharacters(in: CharacterSet(charactersIn: "\"")) }
	}
}

class CsvRowReader {
	
	private let fileStreamReader: FileStreamReader
	private let keys: [String]
	
	init?(fileStreamReader: FileStreamReader) {
		guard let firstLine = fileStreamReader.nextLine() else {
			return nil
		}
		
		self.keys = firstLine.csvComponents()
		self.fileStreamReader = fileStreamReader
	}
	
	func nextRow() -> [String: String]? {
		guard let nextLine = fileStreamReader.nextLine() else {
			return nil
		}
		
		var row: [String: String] = [:]
		var components = nextLine.csvComponents()
		
		for (index, key) in keys.enumerated() {
			row[key] = components[index]
		}
		
		return row
	}
}

func loadCategorizedWords(from fileUrl: URL) -> [String: String] {
	
	var categorizedWords: [String: String] = [:]
	
	guard
		let fileStreamReader = FileStreamReader(url: fileUrl),
		let csvRowReader = CsvRowReader(fileStreamReader: fileStreamReader) else {
			return categorizedWords
	}
	
	while let row = csvRowReader.nextRow() {
		if
			let word = row["word"],
			let category = row["category"],
			category != "uncategorized" {
			
			categorizedWords[word] = category
		}
	}
	
	return categorizedWords
}

guard CommandLine.arguments.count >= 2 else {
	print("Usage: main.swift <filename>")
	exit(1)
}

let inputFilePath = CommandLine.arguments[1]
let inputFileUrl = URL(fileURLWithPath: inputFilePath)

guard FileManager.default.fileExists(atPath: inputFilePath) else {
	print("File \(inputFileUrl.lastPathComponent) not found!")
	exit(1)
}

guard
	let fileStreamReader = FileStreamReader(url: inputFileUrl),
	let csvRowReader = CsvRowReader(fileStreamReader: fileStreamReader) else {
		
		print("File \(inputFileUrl.lastPathComponent) couldn't be read!")
		exit(1)
}

let categorizedWordsFileUrl = URL(fileURLWithPath: "categorized_words.csv")
var graph = Graph(categorizedWords: loadCategorizedWords(from: categorizedWordsFileUrl))

while let row = csvRowReader.nextRow() {
	graph.addNodesAndEdges(fromRow: row)
}

let nodesFileUrl = inputFileUrl.deletingLastPathComponent().appendingPathComponent(inputFileUrl.deletingPathExtension().lastPathComponent + "_nodes.csv")
let edgesFileUrl = inputFileUrl.deletingLastPathComponent().appendingPathComponent(inputFileUrl.deletingPathExtension().lastPathComponent + "_edges.csv")

do {
	try graph.nodesCsv().write(to: nodesFileUrl, atomically: false, encoding: .utf8)
	print("Wrote nodes to \(nodesFileUrl.absoluteString)")
}
catch {
	print("Couldn't write to \(nodesFileUrl.absoluteString)")
	exit(1)
}

do {
	try graph.edgesCsv().write(to: edgesFileUrl, atomically: false, encoding: .utf8)
	print("Wrote edges to \(edgesFileUrl.absoluteString)")
}
catch {
	print("Couldn't write to \(edgesFileUrl.absoluteString)")
	exit(1)
}

do {
	try graph.categorizedWordsCsv().write(to: categorizedWordsFileUrl, atomically: false, encoding: .utf8)
	print("Wrote categorized words to \(categorizedWordsFileUrl.absoluteString)")
}
catch {
	print("Couldn't write to \(categorizedWordsFileUrl.absoluteString)")
	exit(1)
}
