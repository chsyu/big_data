import { useState, useEffect } from 'react';
import './App.css';
import axios from 'axios';

const App = () => {
  const [fileNames, setFileNames] = useState([]);
  const [dragging, setDragging] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [fileProcessingStatus, setFileProcessingStatus] = useState({
    processed_files: [],
  });

  const handleDrop = (event) => {
    event.preventDefault();
    const files = Array.from(event.dataTransfer.files);
    const names = files.map((file) => file.name);
    setFileNames([...fileNames, ...names]);
  };

  const handleDragOver = (event) => {
    event.preventDefault();
    setDragging(true);
  };

  const handleDragLeave = () => {
    setDragging(false);
  };

  const handleSubmit = async () => {
    setIsProcessing(true);  // 開始顯示 spinner
    try {
      const response = await axios.post('http://127.0.0.1:5001/upload_gzfiles/',
        fileNames
      );
      setFileNames([]);
      console.log('Response:', response.data);
    } catch (error) {
      console.error('Error uploading file names:', error);
    }
  };

  // 定期輪詢以檢查後端狀態
  useEffect(() => {
    let interval;
    if (isProcessing) {
      interval = setInterval(async () => {
        try {
          const response = await axios.get('http://127.0.0.1:5001/get_file_processing_status/');
          const status = response.data;

          // 更新狀態
          setFileProcessingStatus(status);

          // 如果狀態變成 completed，停止輪詢
          if (status.status === 'idle') {
            clearInterval(interval);
            setIsProcessing(false);  // 停止顯示 spinner
          }
        } catch (error) {
          console.error('Error fetching file processing status:', error);
        }
      }, 2000); // 每2秒檢查一次
    }
    return () => clearInterval(interval);
  }, [isProcessing]);


  return (
    <div className="App">
      <div
        className={`drop-area ${dragging ? 'dragging' : ''}`}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
      >
        { !(isProcessing || fileProcessingStatus.processed_files.length > 0) && <p className="drop-desc">拖曳檔案到這裡</p>}
        <div className={(isProcessing || fileProcessingStatus.processed_files.length > 0) ? 'message-list' : 'file-list'}>
          {
            (isProcessing || fileProcessingStatus.processed_files.length > 0) ? (fileProcessingStatus.processed_files.map((fileName, index) => (
              <div key={index}>{fileName}
              </div>
            ))
            ) : (fileNames.map((fileName, index) => (
              <div key={index} className="file-name-card">{fileName}
              </div>
            )))
          }
        </div>
      </div>

      <button onClick={handleSubmit}>上傳檔案</button>
      <button onClick={() => {
        setFileNames([])
        setFileProcessingStatus({
          processed_files: [],
        })
        }}>重新開始</button>
    </div>
  );
};

export default App;