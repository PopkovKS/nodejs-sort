const fs = require('fs');
const readline = require('readline');


// Функция для разделения большого файла на несколько временных файлов
async function splitFile(inputFilePath, tmpDir, chunkSize) {

    let outputFile;
    let chunkCount = 0;
    let linesCount = 0;

    const lineReader = readline.createInterface({
        input: fs.createReadStream(inputFilePath),
        crlfDelay: Infinity
    });

    for await (const line of lineReader) {
        if (linesCount % chunkSize === 0) {
            if (outputFile) {
                outputFile.end();
            }
            chunkCount++;
            outputFile = fs.createWriteStream(`${tmpDir}/chunk_${chunkCount}.txt`);
            outputFile.write(`${line}\n`);
        }
        linesCount++;
    }
    return chunkCount;
}

// Функция для сортировки временных файлов и объединения их в итоговый файл
async function mergeFiles(tmpDir, chunkCount, outputFilePath) {
    const chunkFiles = [];

    // Открытие файловых потоков для чтения каждого временного файла
    for (let i = 1; i <= chunkCount; i++) {
        chunkFiles.push(readline.createInterface({
            input: fs.createReadStream(`${tmpDir}/chunk_${i}.txt`),
            crlfDelay: Infinity
        }));
    }

    // Создание файла для записи отсортированных данных
    const outputFile = fs.createWriteStream(outputFilePath);

    const heap = [];

    // Чтение первых строк каждого временного файла и помещение их в кучу
    for (let i = 0; i < chunkCount; i++) {
        const lineReader = chunkFiles[i];

        let lineIterator = await lineReader[Symbol.asyncIterator]();
        let nextLine = await lineIterator.next();

        if (!nextLine.done) {
            const line = nextLine.value;
            heap.push({ line, readerIndex: i });
        }
    }

    // Сортировка и запись строк в итоговый файл
    while (heap.length > 0) {
        heap.sort((a, b) => new Intl.Collator().compare(a.line, b.line));
        // heap.sort((a, b) => a.line.localeCompare(b.line));
        const smallest = heap.shift();
        const lineReader = chunkFiles[smallest.readerIndex];

        outputFile.write(`${smallest.line}\n`);

        const lineIterator = await lineReader[Symbol.asyncIterator]();
        const nextLine = await lineIterator.next();

        if (!nextLine.done) {
            heap.push({line: nextLine.value, readerIndex: smallest.readerIndex});
        }
    }

    // Закрытие файловых потоков
    for (const chunkFile of chunkFiles) {
        chunkFile.close();
    }
    outputFile.end();

    return outputFilePath;
}

// Основная функция, которая выполняет сортировку файла
async function sortLargeFile(inputFilePath, outputFilePath, chunkSize) {
    const tmpDir = './tmp';
    fs.mkdirSync(tmpDir);

    try {
        const chunkCount = await splitFile(inputFilePath, tmpDir, chunkSize);
        await mergeFiles(tmpDir, chunkCount, outputFilePath);
        console.log('Файл отсортирован.');
    } catch (error) {
        console.error('Произошла ошибка при сортировке файла:', error);
    } finally {
        fs.rmSync(tmpDir, {recursive: true});
    }
}

sortLargeFile('data_string.rtf', 'sorted_file.txt', 1);