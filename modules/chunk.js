const chunk = (arr, size) => {
  let chunks = []
  let index = 0
  while (true) {
    const nextChunk = arr.slice(index, index + size)
    if (!nextChunk.length) break
    chunks.push(nextChunk)
    index = index + size
  }
  return chunks
}

module.exports = chunk
