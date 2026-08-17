# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Pham Nam Khanh  **Lớp:** 3B_E403  **Ngày:** 17/8/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 17.7s
  run 2/3 … 16.8s
  run 3/3 … 16.6s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng số dòng khi pipeline được chạy lại hoặc retry, dù bảng có grain 1 dòng / 1 ticket. Điều này tạo các `ticket_id` bị lặp. |
| **Nguyên nhân** | Model incremental không khai báo khóa duy nhất và chiến lược ghi theo khóa, nên dbt ghi thêm dữ liệu bằng insert/append. Khi cùng một ticket xuất hiện lại do retry hoặc CDC update, dòng cũ không được cập nhật mà bị thêm dòng mới. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, giữ `materialized = 'incremental'`, thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`; giữ nguyên bộ lọc `run_date`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False` và `max_active_runs=1` để không chạy bù hàng loạt và không cho nhiều DAG run ghi đồng thời. |
| **Bằng chứng** | Trước: **38.750 hàng** · Sau: **12.480 hàng** · checksum 3 lượt hiện tại: `8dd7c98653`, `8dd7c98653`, `8dd7c98653` · ticket trùng: **0** · DAG: `False / 1` |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ban đầu có 8.645 dòng thay vì 9.100 dòng; các cặp `(event_date, customer_id)` bị thiếu tập trung ở những ngày cũ do dữ liệu đến kho muộn. |
| **P99 độ trễ đo được** | **2.7258333333333336 ngày** *(xấp xỉ 2.73 ngày)* |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 khoảng 2.73 ngày để bao phủ phần lớn dữ liệu đến muộn. |
| **Nguyên nhân** | Điều kiện incremental chỉ lấy `event_date` lớn hơn ngày lớn nhất trong bảng đích. Vì vậy event xảy ra ở ngày cũ nhưng được ingest muộn không được tính lại và bị bỏ sót. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql`, đổi bộ lọc incremental thành lookback 3 ngày: `event_date >= max(event_date) - interval 3 day`. Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để các cặp được tính lại cập nhật dòng cũ thay vì tạo dòng trùng. |
| **Bằng chứng** | trước: **8.645 hàng** · sau: **9.100 hàng** · checksum 3 lượt sau sửa: `3db448685c`, `3db448685c`, `3db448685c` |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 bao phủ khoảng 99% độ trễ quan sát được nhưng không bị chi phối bởi một
> outlier cực lớn như `max`. Vì vậy lookback 3 ngày cân bằng độ đầy đủ dữ liệu
> với chi phí quét và tính lại ở mỗi lần chạy. Dùng `max` có thể bao phủ thêm
> các trường hợp cực đoan nhưng làm mọi lượt chạy sau phải xử lý một window lớn
> hơn đáng kể.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Backend thay đổi cách biểu diễn `priority` từ số sang nhãn chuỗi. Logic cũ dùng `try_cast`, khiến các nhãn hợp lệ thành `NULL`, đồng thời vẫn chấp nhận các số ngoài miền như `0`, `5`, `-1`. |
| **Nguyên nhân** | Silver chưa có logic chuẩn hóa theo data contract: model chỉ ép kiểu bằng `try_cast`, không phân biệt schema evolution hợp lệ với dữ liệu lỗi; ngoài ra việc xếp hạng trước khi lọc có thể làm mất cả ticket nếu bản ghi mới nhất bị lỗi. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Số `1,2,3,4`: giữ nguyên. Nhãn `urgent, high, medium, low`: map lần lượt thành `1,2,3,4`. Giá trị `P1, unknown, 0, 5, -1, rỗng, NULL`: trả về `NULL` và đưa vào quarantine. |
| **Cách khắc phục** | Trong `dbt/macros/normalize_priority.sql`, dùng `CASE` để chuẩn hóa và trả `NULL` cho giá trị lỗi. Trong `silver_tickets.sql`, lọc bản ghi lỗi trước rồi mới `row_number()`. Trong `quarantine_tickets.sql`, lọc các dòng macro trả về `NULL`. Trong `schema.yml`, bật `contract.enforced: true` và thêm test `not_null` cùng `accepted_values: [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** · `dbt test` **11/11 pass** · `silver_tickets.priority` sạch · checksum quarantine 3 lượt: `ebb89036fb` giống nhau |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên giữ nguyên dữ liệu thô ở Bronze để bảo toàn bằng chứng và phục vụ điều
> tra; chuẩn hóa, kiểm tra contract và quarantine nên thực hiện ở Silver. Không
> nên dừng cả pipeline vì chỉ có một số lượng nhỏ bản ghi lỗi trong khi phần
> lớn dữ liệu hợp lệ vẫn cần được phục vụ. Quarantine cô lập 312 bản ghi lỗi
> để xử lý sau mà không chặn toàn bộ DAG.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A / B / không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain và khóa duy nhất của bảng, sau đó kiểm tra model incremental có idempotent hay không bằng cách chạy lại cùng dữ liệu và so sánh số dòng, bản ghi trùng và checksum. |
| 2 | Đo độ trễ giữa thời điểm sự kiện xảy ra và thời điểm dữ liệu được ingest; dùng P99 để chọn lookback window và kiểm tra dữ liệu đến muộn có được cập nhật lại hay không. |
| 3 | Kiểm tra contract, miền giá trị và sự thay đổi schema ở nguồn; phân biệt dữ liệu hợp lệ do schema evolution với dữ liệu lỗi, rồi xác nhận bản ghi lỗi được quarantine mà không làm dừng pipeline. |
