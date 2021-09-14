// New Code
// https://stackoverflow.com/questions/27220859/record-audio-from-user-and-save-to-server

// const GET_END_POINT = 'https://jsonplaceholder.typicode.com/posts/1'
const GET_END_POINT = "http://localhost:8000/fetch-text";
// const POST_END_POINT = 'https://jsonplaceholder.typicode.com/posts'
const POST_END_POINT = "http://localhost:8000/upload-audio";

let message = document.getElementById("text-corpus");
let new_message_button = document.getElementById("new-text-btn");

// Initialize dumme message
let message_recived = {
  id: "1",
  text: "",
};

const updateMessage = (new_message) => {
  message.innerText = new_message.text;
  return;
};
updateMessage(message_recived);

const loading = () => {
  message.innerText = "Loading ...";
};

const button_status = () => {
  if (!message_recived.text) {
    record.classList.add("hidden");
    stop.classList.add("hidden");
  } else {
    record.classList.remove("hidden");
    stop.classList.remove("hidden");
  }
};

const get_message = () => {
  axios
    .get(GET_END_POINT)
    .then(function (response) {
      console.log(response.data);
      message_recived = {
        id: response.data.id,
        text: response.data.text,
      };
      updateMessage(message_recived);
      console.log("Axios");

      button_status();
    })
    .catch(function (error) {
      console.error(error);
    });
};

const send_audio = (blob) => {
  the_audio_data = { userId: "1211", title: "send audio", body: "body audio" };

  // use this to send audio file
  let audioData = new FormData();
  audioData.append("id", message_recived.id);
  audioData.append("text", message_recived.text);
  audioData.append("audio", blob);

  axios.post(POST_END_POINT, audioData).then((res) => {
    console.log(res);

    // reset the message
    message_recived = {
      id: "1",
      text: "",
    };

    // remove the all audio files.
    alert("Audio Saved!");
    setTimeout(() => {
      window.location.reload();
    }, 1000);
  });
};

new_message_button.addEventListener("click", (e) => {
  // Send api request to fetch message
  loading();
  get_message();
});

// set up basic variables for app

const record = document.querySelector(".record");
const stop = document.querySelector(".stop");
const soundClips = document.querySelector(".sound-clips");
const canvas = document.querySelector(".visualizer");
const mainSection = document.querySelector(".main-controls");

// disable stop button while not recording

stop.disabled = true;

// visualiser setup - create web audio api context and canvas

let audioCtx;
const canvasCtx = canvas.getContext("2d");

//main block for doing the audio recording

if (navigator.mediaDevices.getUserMedia) {
  console.log("getUserMedia supported.");

  const constraints = { audio: true };
  let chunks = [];

  let onSuccess = function (stream) {
    const mediaRecorder = new MediaRecorder(stream);

    visualize(stream);

    record.onclick = function () {
      mediaRecorder.start();
      console.log(mediaRecorder.state);
      console.log("recorder started");
      record.style.background = "red";

      stop.disabled = false;
      record.disabled = true;
    };

    stop.onclick = function () {
      mediaRecorder.stop();
      console.log(mediaRecorder.state);
      console.log("recorder stopped");
      record.style.background = "";
      record.style.color = "";
      // mediaRecorder.requestData();

      stop.disabled = true;
      record.disabled = false;
    };

    mediaRecorder.onstop = function (e) {
      console.log("data available after MediaRecorder.stop() called.");

      const clipName = prompt(
        "Enter a name for your sound clip?",
        "My unnamed clip"
      );

      const clipContainer = document.createElement("article");
      const clipLabel = document.createElement("p");
      const audio = document.createElement("audio");
      const deleteButton = document.createElement("button");
      // new code
      const sendButton = document.createElement("button");

      clipContainer.classList.add("clip");
      audio.setAttribute("controls", "");
      deleteButton.textContent = "Delete";
      deleteButton.className = "delete";
      // new code
      sendButton.textContent = "Send";
      sendButton.className = "send";

      if (clipName === null) {
        clipLabel.textContent = "My unnamed clip";
      } else {
        clipLabel.textContent = clipName;
      }

      clipContainer.appendChild(audio);
      clipContainer.appendChild(clipLabel);
      clipContainer.appendChild(sendButton); // new code
      clipContainer.appendChild(deleteButton);
      soundClips.appendChild(clipContainer);

      audio.controls = true;
      console.log("Audio chunks => ", chunks);
      const blob = new Blob(chunks, { type: "audio/wav" });
      chunks = [];
      const audioURL = window.URL.createObjectURL(blob);
      audio.src = audioURL;
      console.log("recorder stopped");

      deleteButton.onclick = function (e) {
        let evtTgt = e.target;
        evtTgt.parentNode.parentNode.removeChild(evtTgt.parentNode);
      };

      // new code
      sendButton.onclick = function (e) {
        send_audio(blob);
      };

      clipLabel.onclick = function () {
        const existingName = clipLabel.textContent;
        const newClipName = prompt("Enter a new name for your sound clip?");
        if (newClipName === null) {
          clipLabel.textContent = existingName;
        } else {
          clipLabel.textContent = newClipName;
        }
      };
    };

    mediaRecorder.ondataavailable = function (e) {
      chunks.push(e.data);
    };
  };

  let onError = function (err) {
    console.log("The following error occured: " + err);
  };

  navigator.mediaDevices.getUserMedia(constraints).then(onSuccess, onError);
} else {
  console.log("getUserMedia not supported on your browser!");
}

function visualize(stream) {
  if (!audioCtx) {
    audioCtx = new AudioContext();
  }

  const source = audioCtx.createMediaStreamSource(stream);

  const analyser = audioCtx.createAnalyser();
  analyser.fftSize = 2048;
  const bufferLength = analyser.frequencyBinCount;
  const dataArray = new Uint8Array(bufferLength);

  source.connect(analyser);
  //analyser.connect(audioCtx.destination);

  draw();

  function draw() {
    const WIDTH = canvas.width;
    const HEIGHT = canvas.height;

    requestAnimationFrame(draw);

    analyser.getByteTimeDomainData(dataArray);

    canvasCtx.fillStyle = "rgb(200, 200, 200)";
    canvasCtx.fillRect(0, 0, WIDTH, HEIGHT);

    canvasCtx.lineWidth = 2;
    canvasCtx.strokeStyle = "rgb(0, 0, 0)";

    canvasCtx.beginPath();

    let sliceWidth = (WIDTH * 1.0) / bufferLength;
    let x = 0;

    for (let i = 0; i < bufferLength; i++) {
      let v = dataArray[i] / 128.0;
      let y = (v * HEIGHT) / 2;

      if (i === 0) {
        canvasCtx.moveTo(x, y);
      } else {
        canvasCtx.lineTo(x, y);
      }

      x += sliceWidth;
    }

    canvasCtx.lineTo(canvas.width, canvas.height / 2);
    canvasCtx.stroke();
  }
}

window.onresize = function () {
  canvas.width = mainSection.offsetWidth;
};

window.onresize();

// put for initial loading.
button_status();
