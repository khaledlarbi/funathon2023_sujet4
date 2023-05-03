import matplotlib.pyplot as plt

import cv2
from pyzbar import pyzbar
from skimage import io


# url = "https://barcode-list.com/barcodeImage.php?barcode=5000112602999"
# url = 'barcode4.png'


def draw_barcode(decoded, image):
    image = cv2.rectangle(
        image,
        (decoded.rect.left, decoded.rect.top),
        (
            decoded.rect.left + decoded.rect.width,
            decoded.rect.top + decoded.rect.height,
        ),
        color=(0, 255, 0),
        thickness=5,
    )
    return image


def extract_ean(url, verbose=True):
    img = io.imread(url)
    decoded_objects = pyzbar.decode(img)
    if verbose is True:
        for obj in decoded_objects:
            # draw the barcode
            print("detected barcode:", obj)
            # print barcode type & data
            print("Type:", obj.type)
            print("Data:", obj.data)
    return decoded_objects

from pyzbar.pyzbar import decode
import numpy as np

def visualise_barcode(url):
    img = io.imread(url)
    for d in decode(img):
        img = cv2.rectangle(img, (d.rect.left, d.rect.top),
                            (d.rect.left + d.rect.width, d.rect.top + d.rect.height), (255, 0, 0), 2)
        img = cv2.polylines(img, [np.array(d.polygon)], True, (0, 255, 0), 2)
        img = cv2.putText(img, d.data.decode(), (d.rect.left, d.rect.top + d.rect.height),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 255), 1, cv2.LINE_AA)

    return img


