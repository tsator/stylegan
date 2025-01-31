import boto3
import cv2
from pathlib import Path
from typing import Optional, List

def read_image(image_path: str) -> Optional[cv2.Mat]:
    """
    Lee una imagen desde la ruta especificada.
    
    Args:
        image_path: Ruta al archivo de imagen
        
    Returns:
        La imagen como un array numpy si tiene éxito, None en caso contrario
    """
    try:
        img = cv2.imread(str(image_path))
        if img is None:
            print(f"Error al leer la imagen: {image_path}")
            return None
        return img
    except Exception as e:
        print(f"Error al leer la imagen {image_path}: {e}")
        return None

def get_images(s3_folder: str, local_download_folder: str) -> List[Path]:
    """
    Descarga todas las imágenes desde el bucket S3 a una carpeta local.
    
    Args:
        s3_folder: La ruta de la carpeta S3 desde donde descargar.
        local_download_folder: Carpeta local para guardar las imágenes descargadas.
        
    Returns:
        Lista de rutas a las imágenes descargadas.
    """
    s3_bucket_name = "avedian-ml"
    downloaded_images = []
    
    try:
        s3 = boto3.client('s3')
        local_folder = Path(local_download_folder)
        local_folder.mkdir(parents=True, exist_ok=True)

        continuation_token = None
        while True:
            if continuation_token:
                response = s3.list_objects_v2(
                    Bucket=s3_bucket_name, 
                    Prefix=s3_folder, 
                    ContinuationToken=continuation_token
                )
            else:
                response = s3.list_objects_v2(
                    Bucket=s3_bucket_name, 
                    Prefix=s3_folder
                )
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith(('.png', '.jpg', '.jpeg')):
                    local_path = local_folder / Path(key).name
                    s3.download_file(s3_bucket_name, key, str(local_path))
            
            # Verificar si hay más objetos para descargar
            if response.get('IsTruncated'):  # Si hay más objetos, se truncó la lista
                continuation_token = response['NextContinuationToken']
            else:
                break
        

    except Exception as e:
        print(f"Error al descargar imágenes: {e}")


def process_images(local_download_folder: str, target_size):
    """
    Procesa todas las imágenes en la carpeta especificada redimensionándolas y elimina los originales.
    """
    folder_path = Path(local_download_folder)
    image_files = [f for f in folder_path.glob("*") if f.suffix.lower() in ('.png', '.jpg', '.jpeg')]
    
    for img_path in image_files:
        try:
            img_array = read_image(str(img_path))
            if img_array is None:
                continue
            
            output_path = folder_path / f"resized_{img_path.name}"
            img_array_resized = cv2.resize(img_array, target_size, interpolation=cv2.INTER_CUBIC)
            
            # Guardar y verificar
            cv2.imwrite(str(output_path), img_array_resized)
            if output_path.exists() and output_path.stat().st_size > 0:
                img_path.unlink(missing_ok=True)
            
        except Exception as e:
            print(f"Error al procesar {img_path.name}: {e}")
            
    print(f"Imágenes cambiadas de tamaño en {local_download_folder}")