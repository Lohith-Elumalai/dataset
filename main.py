from flask import Flask, request, render_template
import torch
from PIL import Image
import torchvision.transforms as transforms
import torchvision.models as models

app = Flask(__name__)

# Load pre-trained model
model = models.densenet121(pretrained=True)
model.eval()

# Load pre-processing transforms
transform = transforms.Compose([transforms.Resize(256),
                                transforms.CenterCrop(224),
                                transforms.ToTensor(),
                                transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                                     std=[0.229, 0.224, 0.225])])

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Read image file
        img = Image.open(request.files['file'])
        # Pre-process image
        img_t = transform(img)
        # Create batch
        batch_t = torch.unsqueeze(img_t, 0)
        # Get features
        features = model.features(batch_t).view(1, -1)
        # Get class label
        output = model.classifier(features)
        _, predicted = torch.max(output, 1)
        # Render HTML with result
        return render_template('index.html', result=predicted.item())
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
