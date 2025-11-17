# 📋 TÀI LIỆU RÀNG BUỘC VRP OPTIMIZER

## Tổng quan
Tài liệu này liệt kê TẤT CẢ các ràng buộc đã được implement trong hệ thống VRP Optimizer.

---

## 🔴 HARD CONSTRAINTS (Ràng buộc cứng - KHÔNG THỂ vi phạm)

### 1. **Ràng buộc Capacity (Sức chứa)**
- **Mô tả**: Tổng trọng lượng và thể tích đơn hàng không được vượt quá khả năng của xe
- **Công thức**:
  - `currentWeight + customer.getOrderWeight() ≤ vehicle.getCapacityWeight()`
  - `currentVolume + customer.getOrderVolume() ≤ vehicle.getCapacityVolume()`
- **Vị trí code**: `Route.java:54-59`
- **Xử lý vi phạm**: Từ chối thêm khách hàng vào route

### 2. **Ràng buộc Distance (Khoảng cách tối đa)**
- **Mô tả**: Tổng quãng đường của route không được vượt quá khoảng cách tối đa của xe
- **Công thức**: 
  - `totalDistance + roadDistance + returnDistanceEstimate ≤ vehicle.getMaxDistance()`
  - Return distance estimate = `roadDistance * 0.5`
- **Vị trí code**: `Route.java:61-70`
- **Xử lý vi phạm**: Từ chối thêm khách hàng

### 3. **Ràng buộc Road Restrictions (Hạn chế đường)**
- **Mô tả**: Một số loại xe không được phép đi trên một số đường
- **Quy tắc**:
  - Đường có restriction "No Heavy Trucks" → Truck và Van không được đi
  - Đường không có restriction → Tất cả xe đều được đi
- **Vị trí code**: `Road.java:30-42`, `Route.java:45-48`
- **Xử lý vi phạm**: Từ chối sử dụng đường đó

### 4. **Ràng buộc Vehicle Compatibility (Tương thích xe)**
- **Mô tả**: Một số loại xe chỉ phục vụ một số loại delivery cụ thể
- **Quy tắc**:
  - **Bike** chỉ phục vụ **Home delivery**
  - Các loại xe khác có thể phục vụ tất cả loại delivery
- **Vị trí code**: `Route.java:110-118`
- **Xử lý vi phạm**: Từ chối gán khách hàng cho xe không tương thích

### 5. **Ràng buộc Working Hours (Giờ làm việc của Depot)**
- **Mô tả**: Route phải hoàn thành trước khi depot đóng cửa
- **Công thức**: 
  - `serviceEndTime ≤ depot.getCloseTime()`
  - `serviceEndTime = arrivalTime + customer.getServiceTime()`
- **Vị trí code**: `Route.java:86-90`
- **Xử lý vi phạm**: Từ chối thêm khách hàng

### 6. **Ràng buộc Maximum Route Duration (Thời gian route tối đa)**
- **Mô tả**: Mỗi route không được kéo dài quá 8 giờ
- **Giá trị**: `MAX_ROUTE_DURATION_HOURS = 8` giờ
- **Công thức**: 
  - `routeDuration ≤ 8 * 60` phút
  - `routeDuration = serviceEndTime - routeStartTime`
- **Vị trí code**: `Route.java:24, 92-96`
- **Xử lý vi phạm**: Từ chối thêm khách hàng

### 7. **Ràng buộc Driver Rest Time (Thời gian nghỉ của tài xế)**
- **Mô tả**: Tài xế phải nghỉ 30 phút sau khi lái 4 giờ
- **Giá trị**:
  - `REST_TRIGGER_HOURS = 4` giờ
  - `DRIVER_REST_TIME_MINUTES = 30` phút
- **Công thức**:
  - Nếu `totalTravelTime + newTravelTime ≥ 4 giờ` và chưa nghỉ → Phải nghỉ 30 phút
  - `afterRestTime = serviceEndTime + 30 phút ≤ depot.getCloseTime()`
- **Vị trí code**: `Route.java:25-26, 98-108`
- **Xử lý vi phạm**: Từ chối thêm khách hàng nếu không có thời gian nghỉ

### 8. **Ràng buộc CRITICAL: Clustering (Phân cụm vùng)**
- **Mô tả**: Mỗi route CHỈ được phục vụ khách hàng trong CÙNG MỘT cụm vùng. Tài xế phải chọn một vùng có x khách hàng để giao hàng thành từng cụm, sau đó mới chạy xe về.
- **Quy tắc**:
  - Khách hàng được phân cụm theo vị trí địa lý (K-means clustering)
  - Mỗi route được gán cho một cluster cụ thể
  - Route CHỈ được thêm khách hàng từ cluster đã được gán
  - Không được nhảy từ cụm này sang cụm khác một cách tùy tiện
  - Sau khi phục vụ xong cụm, xe mới quay về depot
- **Tham số**:
  - `TARGET_CLUSTER_SIZE = 10`: Số khách hàng mục tiêu trong mỗi cluster
  - Số cluster được tính tự động: `ceil(totalCustomers / TARGET_CLUSTER_SIZE)`
- **Vị trí code**: 
  - `ClusterManager.java`: Thuật toán K-means clustering
  - `Route.java:22, 49-66, 68-72`: Kiểm tra cluster constraint
  - `VRPSolverAdvanced.java:30-33, 71-113, 128-175`: Logic tạo routes theo cluster
- **Xử lý vi phạm**: Từ chối thêm khách hàng từ cluster khác
- **Gộp routes**: Chỉ được gộp routes từ cùng cluster

### 9. **Ràng buộc CRITICAL: Tất cả khách hàng phải được phục vụ**
- **Mô tả**: Đây là ràng buộc QUAN TRỌNG NHẤT - 100% khách hàng phải được gán vào route
- **Vị trí code**: `VRPSolverAdvanced.java:274-350`
- **Xử lý**:
  - Nếu không thể gán vào route hiện có → Tạo route riêng cho cluster đó
  - Nếu vi phạm soft constraints → Force add với penalty lớn
  - Nếu vi phạm hard constraints → Báo lỗi nghiêm trọng
- **Kiểm tra**: Sau mỗi phase, kiểm tra lại tất cả khách hàng

---

## 🟡 SOFT CONSTRAINTS (Ràng buộc mềm - CÓ THỂ vi phạm với PENALTY)

### 10. **Ràng buộc Time Window (Cửa sổ thời gian) - SOFT**
- **Mô tả**: Khách hàng có cửa sổ thời gian mong muốn, nhưng có thể vi phạm với penalty
- **Quy tắc**:
  - **Early Arrival** (đến sớm): 
    - Penalty: `0.1 * số phút đến sớm`
    - Xe phải đợi đến khi time window mở
  - **Late Arrival** (đến muộn):
    - Penalty: `0.5 * số phút đến muộn`
    - Cho phép muộn tối đa **30 phút** (hard limit)
    - Nếu muộn > 30 phút → Từ chối (hard constraint)
- **Giá trị**:
  - `TIME_WINDOW_PENALTY_RATE = 0.5` (cost/phút muộn)
  - `EARLY_ARRIVAL_PENALTY_RATE = 0.1` (cost/phút sớm)
  - `MAX_TIME_WINDOW_VIOLATION = 30` phút
- **Vị trí code**: `Route.java:22-23, 72-84, 133-143`
- **Xử lý vi phạm**: 
  - Cho phép vi phạm với penalty
  - Nếu vi phạm quá nghiêm trọng (>30 phút) → Từ chối

### 11. **Ràng buộc Traffic-Aware Travel Time (Thời gian di chuyển theo giao thông)**
- **Mô tả**: Thời gian di chuyển được điều chỉnh theo mức độ giao thông
- **Quy tắc**:
  - **Low Traffic**: `travelTime = baseTravelTime * 1.0` (không đổi)
  - **Medium Traffic**: `travelTime = baseTravelTime * 1.15` (+15%)
  - **High Traffic**: `travelTime = baseTravelTime * 1.3` (+30%)
- **Vị trí code**: `Road.java:44-53`
- **Ảnh hưởng**: Ảnh hưởng đến time window và route duration

### 12. **Ràng buộc Route Duration Penalty (Phạt thời gian route dài)**
- **Mô tả**: Routes dài hơn sẽ bị penalty trong scoring (không phải hard constraint)
- **Công thức**: `durationPenalty = routeDuration / 480.0` (normalize to 8 hours)
- **Vị trí code**: `VRPSolverAdvanced.java:154, 161`
- **Mục đích**: Khuyến khích routes ngắn hơn trong scoring

### 13. **Force Add Penalty (Phạt khi force add customer)**
- **Mô tả**: Khi phải force add customer để đảm bảo phục vụ, sẽ có penalty lớn
- **Quy tắc**:
  - Base penalty: `50.0`
  - Time window violation: `penalty * 2-3` (double/triple)
  - Route duration excess: `1.0 * số phút vượt quá`
- **Vị trí code**: `Route.java:177-234`
- **Mục đích**: Đảm bảo tất cả khách hàng được phục vụ nhưng với chi phí cao

---

## 📊 SCORING FACTORS (Yếu tố tính điểm - không phải ràng buộc)

### 14. **Distance Score (Điểm khoảng cách)**
- **Trọng số**: 35-40%
- **Mục đích**: Ưu tiên khách hàng gần hơn

### 15. **Priority Score (Điểm ưu tiên)**
- **Trọng số**: 25%
- **Công thức**: `1.0 / (priorityLevel + 1)`
- **Mục đích**: Ưu tiên khách hàng có priority cao hơn

### 16. **Time Window Urgency Score (Điểm cấp bách)**
- **Trọng số**: 10-15%
- **Mục đích**: Ưu tiên khách hàng có time window sớm hơn

### 17. **Utilization Bonus (Thưởng sử dụng)**
- **Trọng số**: 10%
- **Công thức**: `1.0 - (weightUtilization * 0.2)`
- **Mục đích**: Khuyến khích routes có utilization cao hơn

### 18. **Traffic Penalty (Phạt giao thông)**
- **Trọng số**: Tích hợp vào distance score
- **Multiplier**: 
  - High: 1.3x
  - Medium: 1.15x
  - Low: 1.0x
- **Mục đích**: Tránh routes qua khu vực giao thông cao

---

## 🔧 CÁC THAM SỐ CẤU HÌNH

| Tham số | Giá trị | Mô tả |
|---------|---------|-------|
| `MAX_ROUTE_DURATION_HOURS` | 8 | Thời gian route tối đa (giờ) |
| `REST_TRIGGER_HOURS` | 4 | Số giờ lái trước khi phải nghỉ |
| `DRIVER_REST_TIME_MINUTES` | 30 | Thời gian nghỉ bắt buộc (phút) |
| `TIME_WINDOW_PENALTY_RATE` | 0.5 | Phí penalty mỗi phút muộn |
| `EARLY_ARRIVAL_PENALTY_RATE` | 0.1 | Phí penalty mỗi phút sớm |
| `MAX_TIME_WINDOW_VIOLATION` | 30 | Thời gian muộn tối đa cho phép (phút) |
| `TRAFFIC_HIGH_MULTIPLIER` | 1.3 | Hệ số thời gian khi giao thông cao |
| `TRAFFIC_MEDIUM_MULTIPLIER` | 1.15 | Hệ số thời gian khi giao thông trung bình |
| `FORCE_ADD_BASE_PENALTY` | 50.0 | Penalty cơ bản khi force add |
| `TARGET_CLUSTER_SIZE` | 10 | Số khách hàng mục tiêu trong mỗi cluster |

---

## 📝 GHI CHÚ QUAN TRỌNG

1. **Hard Constraints**: Không thể vi phạm, nếu vi phạm → Từ chối
2. **Soft Constraints**: Có thể vi phạm với penalty, nhưng có giới hạn
3. **Critical Constraint**: Tất cả khách hàng PHẢI được phục vụ - đây là ưu tiên cao nhất
4. **Force Add**: Chỉ được sử dụng khi cần đảm bảo tất cả khách hàng được phục vụ
5. **Penalties**: Tất cả penalties được cộng vào tổng chi phí và hiển thị trong báo cáo

---

## 🔍 VỊ TRÍ CODE CHI TIẾT

- **Route.java**: Tất cả ràng buộc route-level (lines 44-234)
- **Road.java**: Ràng buộc road restrictions và traffic (lines 30-53)
- **VRPSolverAdvanced.java**: Logic đảm bảo tất cả khách hàng được phục vụ (lines 209-350)
- **Customer.java**: Model khách hàng với các thuộc tính ràng buộc
- **Vehicle.java**: Model xe với capacity và max distance
- **Depot.java**: Model depot với operating hours

---

**Ngày tạo**: 2024
**Phiên bản**: 1.0
**Tác giả**: VRP Optimizer System

