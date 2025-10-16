from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import  date
from fastapi.responses import Response
import pandas as pd
import io
from app.database.database import get_db
from ..config import redis_client
from app.cache.cache import RedisCache
from ..services.index_calculator import IndexCalculator

router = APIRouter()
cache = RedisCache(redis_client)
@router.post("/build-index")
async def build_index(
    start_date: date = Query(..., description="Start date for index building"),
    end_date: date = Query(..., description="End date for index building"),
    session: AsyncSession = Depends(get_db)
):
    """Build the index for a specified date range using stored data"""
    try:
        calculator = IndexCalculator(session, cache)
        result = await calculator.build_index(start_date, end_date)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/index-performance")
async def get_index_performance_range(
    start_date: date = Query(..., description="Start date for performance data"),
    end_date: date = Query(..., description="End date for performance data"),
    db: AsyncSession = Depends(get_db)
):
    """Get index performance for a date range (cached)"""
    calculator = IndexCalculator(db, cache)
    performances = await calculator.get_performance(start_date, end_date)

    if not performances:
        raise HTTPException(
            status_code=404,
            detail="No performance data available for the specified date range"
        )

    return performances

@router.get("/index-composition")
async def get_index_composition_by_date(
    date: date = Query(..., description="Date for composition snapshot"),
    db: AsyncSession = Depends(get_db)
):
    """Get index composition for a specific date (cached)"""
    calculator = IndexCalculator(db, cache)
    composition = await calculator.get_composition_for_date(date)

    if not composition:
        print("fdaafd")
        raise HTTPException(
            status_code=404,
            detail=f"No composition data available for date {date}"
        )

    return composition

@router.get("/composition-changes")
async def get_composition_changes(
        start_date: date = Query(..., description="Start date for composition changes"),
        end_date: date = Query(..., description="End date for composition changes"),
    db: AsyncSession = Depends(get_db)
):
    """Get composition changes for a date range (cached)"""
    calculator = IndexCalculator(db, cache)
    composition = await calculator.get_composition_changes(start_date, end_date)
    if not composition:
        raise HTTPException(
            status_code=404,
            detail=f"No composition data available for date {date}"
        )
    return composition

@router.post("/export-data")
async def export_data(
    db: AsyncSession = Depends(get_db)
):
    """Export index performance, composition, and changes data to Excel"""
    try:
        # 1. Fetch daily returns from index_performance
        performance_result = await db.execute(
            text("""
                SELECT date, daily_return
                FROM index_performance 
                ORDER BY date
            """)
        )
        performances = performance_result.fetchall()

        # 2. Fetch all index composition data
        composition_result = await db.execute(
            text("""
                SELECT ic.date, ic.symbol, s.company_name, s.sector, dd.price, dd.market_cap
                FROM index_composition ic
                JOIN stocks s ON ic.symbol = s.symbol
                LEFT JOIN daily_data dd ON ic.symbol = dd.symbol AND ic.date = dd.date
                ORDER BY ic.date, ic.symbol
            """)
        )
        compositions = composition_result.fetchall()

        # 3. Calculate composition changes
        changes_result = await db.execute(
            text("""
                WITH dates AS (
                    SELECT DISTINCT date 
                    FROM index_composition 
                    ORDER BY date
                ),
                curr_comp AS (
                    SELECT d.date as curr_date,
                           LEAD(d.date) OVER (ORDER BY d.date) as next_date,
                           (
                               SELECT GROUP_CONCAT(symbol)
                               FROM index_composition ic
                               WHERE ic.date = d.date
                           ) as curr_symbols
                    FROM dates d
                )
                SELECT 
                    curr_date,
                    next_date,
                    curr_symbols as current_composition,
                    (
                        SELECT GROUP_CONCAT(symbol)
                        FROM index_composition ic
                        WHERE ic.date = next_date
                    ) as next_composition
                FROM curr_comp
                WHERE next_date IS NOT NULL
                ORDER BY curr_date
            """)
        )
        composition_changes = changes_result.fetchall()

        # Create Excel file with multiple sheets
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Performance sheet
            perf_df = pd.DataFrame([{
                'Date': perf.date,
                'Daily Return': perf.daily_return
            } for perf in performances])
            perf_df.to_excel(writer, sheet_name='Index Performance', index=False)

            # Composition sheet
            comp_df = pd.DataFrame([{
                'Date': comp.date,
                'Symbol': comp.symbol,
                'Name': comp.company_name,
                'Category': comp.sector,
                'Price': comp.price,
                'Market Cap': comp.market_cap
            } for comp in compositions])
            comp_df.to_excel(writer, sheet_name='Index Composition', index=False)

            # Composition Changes sheet
            changes_data = []
            for change in composition_changes:
                if change.next_composition:  # Skip if there's no next day composition
                    curr_set = set(change.current_composition.split(',')) if change.current_composition else set()
                    next_set = set(change.next_composition.split(',')) if change.next_composition else set()

                    added = next_set - curr_set
                    removed = curr_set - next_set

                    if added or removed:  # Only add to changes if there were actual changes
                        changes_data.append({
                            'Date': change.curr_date,
                            'Next Date': change.next_date,
                            'Added Symbols': ','.join(sorted(added)) if added else '',
                            'Removed Symbols': ','.join(sorted(removed)) if removed else ''
                        })

            changes_df = pd.DataFrame(changes_data)
            if not changes_df.empty:
                changes_df.to_excel(writer, sheet_name='Composition Changes', index=False)

        output.seek(0)

        # Return the Excel file as a response
        return Response(
            content=output.getvalue(),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': 'attachment; filename=index_data.xlsx'
            }
        )

    except Exception as e:
        print(f"Error in export_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
