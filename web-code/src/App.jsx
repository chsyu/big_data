import { useState } from 'react';
import './App.css';
import axios from 'axios';

const App = () => {
  const [fileNames, setFileNames] = useState([]);
  const [dragging, setDragging] = useState(false);

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

  return (
    <div className="App">
      <div
        className={`drop-area ${dragging ? 'dragging' : ''}`}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
      >
        <p className="drop-desc">拖曳檔案到這裡</p>
        <div className="file-list">
          {fileNames.map((fileName, index) => (
            <div key={index} className="file-name-card">{fileName}
            </div>
          ))}
        </div>
      </div>

      <button onClick={handleSubmit}>上傳檔案名稱到後台</button>
      <button onClick={() => setFileNames([])}>清空檔案名稱</button>
    </div>
  );
};

export default App;