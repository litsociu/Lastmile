import pandas as pd
import os
from pathlib import Path

def convert_excel_to_csv(directory_path):
    """
    Chuyển đổi tất cả các file Excel (.xlsx) trong thư mục và các thư mục con
    sang định dạng CSV.
    
    Args:
        directory_path: Đường dẫn đến thư mục chứa các file Excel
    """
    directory = Path(directory_path)
    
    if not directory.exists():
        print(f"❌ Thư mục không tồn tại: {directory_path}")
        return
    
    # Tìm tất cả các file .xlsx trong thư mục và các thư mục con
    excel_files = list(directory.rglob("*.xlsx"))
    
    if not excel_files:
        print(f"⚠️ Không tìm thấy file Excel nào trong thư mục: {directory_path}")
        return
    
    print(f"📂 Tìm thấy {len(excel_files)} file Excel cần chuyển đổi...\n")
    
    converted_count = 0
    deleted_count = 0
    error_count = 0
    
    for excel_file in excel_files:
        try:
            # Đọc file Excel
            df = pd.read_excel(excel_file)
            
            # Tạo tên file CSV (thay đổi extension)
            csv_file = excel_file.with_suffix('.csv')
            
            # Lưu file CSV
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            
            # Xóa file Excel gốc sau khi chuyển đổi thành công
            excel_file.unlink()
            
            print(f"✅ Đã chuyển đổi và xóa: {excel_file.name} -> {csv_file.name}")
            converted_count += 1
            deleted_count += 1
            
        except Exception as e:
            print(f"❌ Lỗi khi chuyển đổi {excel_file.name}: {str(e)}")
            error_count += 1
    
    print(f"\n{'='*60}")
    print(f"📊 Tổng kết:")
    print(f"   ✅ Đã chuyển đổi: {converted_count} file")
    print(f"   🗑️  Đã xóa file Excel gốc: {deleted_count} file")
    print(f"   ❌ Lỗi: {error_count} file")
    print(f"   📁 Tổng số file: {len(excel_files)} file")
    print(f"{'='*60}")

if __name__ == "__main__":
    # Đường dẫn đến thư mục chứa các file Excel
    target_directory = r"D:\A UEH_UNIVERSITY\UEH_Subjects\LMDO\Lastmile\java_optimizer\src\main\resources\LMDO processed"
    
    # Hoặc sử dụng đường dẫn tương đối từ thư mục hiện tại
    # target_directory = Path(__file__).parent
    
    print("🚀 Bắt đầu chuyển đổi Excel sang CSV...\n")
    convert_excel_to_csv(target_directory)
    print("\n✨ Hoàn thành!")

