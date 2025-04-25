#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SCADA Data Analysis Tool
========================
เครื่องมือวิเคราะห์ข้อมูลจากตาราง SCADA_Data ในฐานข้อมูล Electric

โค้ดนี้จะ:
1. เชื่อมต่อกับฐานข้อมูล MSSQL
2. ดึงข้อมูล SCADA_Data
3. วิเคราะห์ข้อมูลรายวันและรายอุปกรณ์ (DUID)
4. สร้างแผนภูมิและกราฟแสดงผลการวิเคราะห์
5. บันทึกผลลัพธ์เป็นไฟล์ CSV และ PDF
"""

import os
import pandas as pd
import numpy as np
import pymssql
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from matplotlib.backends.backend_pdf import PdfPages

# ตั้งค่า style plot
sns.set(style="whitegrid")
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'sans-serif']
plt.rcParams.update({'figure.autolayout': True})

# ข้อมูลการเชื่อมต่อกับฐานข้อมูล
DB_SERVER = '34.134.173.24'
DB_NAME = 'Electric'
DB_USER = 'SA'
DB_PASSWORD = 'Passw0rd123456'

# สร้างโฟลเดอร์สำหรับบันทึกผลลัพธ์
OUTPUT_DIR = 'scada_analysis_results'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def connect_to_db():
    """เชื่อมต่อกับฐานข้อมูล MSSQL"""
    try:
        conn = pymssql.connect(
            server=DB_SERVER,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print(f"เชื่อมต่อกับฐานข้อมูล {DB_NAME} สำเร็จ")
        return conn
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อกับฐานข้อมูล: {e}")
        return None

def get_scada_data(conn):
    """ดึงข้อมูลจากตาราง SCADA_Data"""
    query = """
    SELECT 
        ID, SETTLEMENTDATE, DUID, SCADAVALUE, LASTCHANGED, IMPORT_TIMESTAMP
    FROM 
        SCADA_Data 
    ORDER BY 
        SETTLEMENTDATE
    """
    
    try:
        df = pd.read_sql(query, conn)
        print(f"ดึงข้อมูลจากตาราง SCADA_Data สำเร็จ จำนวน {len(df)} แถว")
        return df
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        return None

def daily_summary(df):
    """สรุปข้อมูลรายวัน"""
    # แปลงคอลัมน์วันที่เป็น datetime
    df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
    
    # สร้างคอลัมน์วันที่ (ไม่มีเวลา)
    df['DATE'] = df['SETTLEMENTDATE'].dt.date
    
    # วิเคราะห์ข้อมูลรายวัน
    daily_stats = df.groupby('DATE').agg({
        'ID': 'count',
        'SCADAVALUE': ['mean', 'min', 'max', 'sum']
    }).reset_index()
    
    # ตั้งชื่อคอลัมน์ใหม่
    daily_stats.columns = ['Date', 'RecordCount', 'AvgValue', 'MinValue', 'MaxValue', 'TotalValue']
    
    return daily_stats

def duid_summary(df):
    """สรุปข้อมูลตาม DUID (อุปกรณ์)"""
    # วิเคราะห์ข้อมูลตาม DUID
    duid_stats = df.groupby('DUID').agg({
        'ID': 'count',
        'SCADAVALUE': ['mean', 'min', 'max', 'sum']
    }).reset_index()
    
    # ตั้งชื่อคอลัมน์ใหม่
    duid_stats.columns = ['DUID', 'RecordCount', 'AvgValue', 'MinValue', 'MaxValue', 'TotalValue']
    
    # เรียงลำดับตามค่าเฉลี่ย
    duid_stats = duid_stats.sort_values(by='AvgValue', ascending=False)
    
    return duid_stats

def hourly_summary(df):
    """สรุปข้อมูลตามชั่วโมง"""
    # เพิ่มคอลัมน์ชั่วโมง
    df['Hour'] = df['SETTLEMENTDATE'].dt.hour
    
    # วิเคราะห์ข้อมูลตามชั่วโมง
    hourly_stats = df.groupby('Hour').agg({
        'ID': 'count',
        'SCADAVALUE': ['mean', 'min', 'max', 'sum']
    }).reset_index()
    
    # ตั้งชื่อคอลัมน์ใหม่
    hourly_stats.columns = ['Hour', 'RecordCount', 'AvgValue', 'MinValue', 'MaxValue', 'TotalValue']
    
    return hourly_stats

def plot_daily_data(daily_stats, duid_stats, hourly_stats):
    """สร้างกราฟแสดงผลการวิเคราะห์"""
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_filename = os.path.join(OUTPUT_DIR, f'scada_analysis_{current_datetime}.pdf')
    
    with PdfPages(pdf_filename) as pdf:
        # 1. กราฟแสดงจำนวนข้อมูลรายวัน
        plt.figure(figsize=(12, 6))
        plt.bar(daily_stats['Date'].astype(str), daily_stats['RecordCount'], color='skyblue')
        plt.title('จำนวนข้อมูล SCADA รายวัน')
        plt.xlabel('วันที่')
        plt.ylabel('จำนวนข้อมูล')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()
        
        # 2. กราฟแสดงค่าเฉลี่ย SCADAVALUE รายวัน
        plt.figure(figsize=(12, 6))
        plt.plot(daily_stats['Date'].astype(str), daily_stats['AvgValue'], marker='o', linestyle='-', color='green')
        plt.title('ค่าเฉลี่ย SCADAVALUE รายวัน')
        plt.xlabel('วันที่')
        plt.ylabel('ค่าเฉลี่ย SCADAVALUE')
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        pdf.savefig()
        plt.close()
        
        # 3. กราฟแสดงค่า SCADAVALUE สูงสุดและต่ำสุดรายวัน
        plt.figure(figsize=(12, 6))
        plt.plot(daily_stats['Date'].astype(str), daily_stats['MaxValue'], marker='^', linestyle='-', color='red', label='ค่าสูงสุด')
        plt.plot(daily_stats['Date'].astype(str), daily_stats['MinValue'], marker='v', linestyle='-', color='blue', label='ค่าต่ำสุด')
        plt.title('ค่า SCADAVALUE สูงสุดและต่ำสุดรายวัน')
        plt.xlabel('วันที่')
        plt.ylabel('SCADAVALUE')
        plt.xticks(rotation=45)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        pdf.savefig()
        plt.close()
        
        # 4. กราฟแสดง Top 10 DUID ที่มีค่าเฉลี่ย SCADAVALUE สูงสุด
        plt.figure(figsize=(12, 6))
        top_duid = duid_stats.head(10)
        plt.bar(top_duid['DUID'], top_duid['AvgValue'], color='orange')
        plt.title('10 อันดับ DUID ที่มีค่าเฉลี่ย SCADAVALUE สูงสุด')
        plt.xlabel('DUID')
        plt.ylabel('ค่าเฉลี่ย SCADAVALUE')
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()
        
        # 5. กราฟแสดงค่าเฉลี่ย SCADAVALUE ตามชั่วโมง
        plt.figure(figsize=(12, 6))
        plt.plot(hourly_stats['Hour'], hourly_stats['AvgValue'], marker='o', linestyle='-', color='purple')
        plt.title('ค่าเฉลี่ย SCADAVALUE ตามชั่วโมง')
        plt.xlabel('ชั่วโมง')
        plt.ylabel('ค่าเฉลี่ย SCADAVALUE')
        plt.xticks(range(0, 24))
        plt.grid(True)
        plt.tight_layout()
        pdf.savefig()
        plt.close()
        
        # 6. กราฟแสดงจำนวนข้อมูลตามชั่วโมง
        plt.figure(figsize=(12, 6))
        plt.bar(hourly_stats['Hour'], hourly_stats['RecordCount'], color='teal')
        plt.title('จำนวนข้อมูล SCADA ตามชั่วโมง')
        plt.xlabel('ชั่วโมง')
        plt.ylabel('จำนวนข้อมูล')
        plt.xticks(range(0, 24))
        plt.tight_layout()
        pdf.savefig()
        plt.close()
    
    print(f"บันทึกกราฟลงในไฟล์ PDF: {pdf_filename}")

def save_to_csv(daily_stats, duid_stats, hourly_stats):
    """บันทึกผลการวิเคราะห์เป็นไฟล์ CSV"""
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # บันทึกข้อมูลรายวัน
    daily_csv = os.path.join(OUTPUT_DIR, f'daily_summary_{current_datetime}.csv')
    daily_stats.to_csv(daily_csv, index=False)
    print(f"บันทึกข้อมูลรายวันลงในไฟล์: {daily_csv}")
    
    # บันทึกข้อมูลตาม DUID
    duid_csv = os.path.join(OUTPUT_DIR, f'duid_summary_{current_datetime}.csv')
    duid_stats.to_csv(duid_csv, index=False)
    print(f"บันทึกข้อมูลตาม DUID ลงในไฟล์: {duid_csv}")
    
    # บันทึกข้อมูลตามชั่วโมง
    hourly_csv = os.path.join(OUTPUT_DIR, f'hourly_summary_{current_datetime}.csv')
    hourly_stats.to_csv(hourly_csv, index=False)
    print(f"บันทึกข้อมูลตามชั่วโมงลงในไฟล์: {hourly_csv}")

def main():
    """ฟังก์ชันหลัก"""
    # เชื่อมต่อกับฐานข้อมูล
    conn = connect_to_db()
    if conn is None:
        return
    
    try:
        # ดึงข้อมูล
        df = get_scada_data(conn)
        if df is None:
            return
        
        # วิเคราะห์ข้อมูล
        print("\nกำลังวิเคราะห์ข้อมูล...")
        daily_stats = daily_summary(df)
        duid_stats = duid_summary(df)
        hourly_stats = hourly_summary(df)
        
        # แสดงผลลัพธ์
        print("\nสรุปข้อมูลรายวัน:")
        print(daily_stats)
        
        print("\nTop 10 DUID ที่มีค่าเฉลี่ย SCADAVALUE สูงสุด:")
        print(duid_stats.head(10))
        
        print("\nสรุปข้อมูลตามชั่วโมง:")
        print(hourly_stats)
        
        # สร้างกราฟ
        print("\nกำลังสร้างกราฟ...")
        plot_daily_data(daily_stats, duid_stats, hourly_stats)
        
        # บันทึกข้อมูล
        print("\nกำลังบันทึกข้อมูล CSV...")
        save_to_csv(daily_stats, duid_stats, hourly_stats)
        
        print(f"\nการวิเคราะห์เสร็จสิ้น ผลลัพธ์ถูกบันทึกไว้ในโฟลเดอร์: {OUTPUT_DIR}")
        
    except Exception as e:
        print(f"เกิดข้อผิดพลาด: {e}")
    finally:
        # ปิดการเชื่อมต่อ
        conn.close()
        print("ปิดการเชื่อมต่อกับฐานข้อมูลแล้ว")

if __name__ == "__main__":
    print("=== เริ่มต้นการวิเคราะห์ข้อมูลจากตาราง SCADA_Data ===\n")
    main()
    print("\n=== สิ้นสุดการวิเคราะห์ข้อมูล ===")