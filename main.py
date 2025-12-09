"""
Основной модуль приложения для обработки месячных данных.

Собирает данные из файлов в каталогах OD, RA, PS,
формирует сводный файл с уникальными табельными номерами
и сохраняет результат в форматированном Excel файле.

Все настройки и конфигурация находятся в этом файле.
"""

import logging
import os
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field

import pandas as pd

# Попытка импортировать openpyxl для форматирования (обычно доступен в Anaconda)
try:
    from openpyxl import load_workbook
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

# Попытка импортировать xlsxwriter для форматирования (если openpyxl недоступен)
try:
    import xlsxwriter
    XLSXWRITER_AVAILABLE = True
except ImportError:
    XLSXWRITER_AVAILABLE = False


# ============================================================================
# НАСТРОЙКИ ПРИЛОЖЕНИЯ
# ============================================================================

# Пути к каталогам
INPUT_DIR = "IN"  # Каталог с входными данными
OUTPUT_DIR = "OUT"  # Каталог для выходных файлов
LOG_DIR = "log"  # Каталог для логов

# Уровень логирования (INFO или DEBUG)
LOG_LEVEL = "DEBUG"  # Уровень логирования: DEBUG (в файлы) - детальное, INFO (в консоль) - верхнеуровневое

# Тема логов (используется в имени файла)
LOG_THEME = "processor"

# Включить/выключить сбор и вывод статистики
ENABLE_STATISTICS = True  # True - собирать статистику и создавать лист "Статистика", False - не собирать


# ============================================================================
# КОНФИГУРАЦИЯ ЗАГРУЗКИ ФАЙЛОВ
# ============================================================================

@dataclass
class DropRule:
    """
    Правило удаления строк.
    
    Параметры:
        alias: Имя поля после маппинга (из default_columns, например "tb", "status")
        values: Список запрещенных значений (строки будут удалены, если значение поля совпадает с одним из них)
        remove_unconditionally: True - удалять всегда (по умолчанию), False - не удалять (правило игнорируется)
        check_by_inn: True - не удалять строку, если по этому ИНН (client_id) есть другие строки с незапрещенными значениями
        check_by_tn: True - не удалять строку, если по этому ТН (tab_number) есть другие строки с незапрещенными значениями
    
    Комбинации:
        - remove_unconditionally=True, check_by_inn=False, check_by_tn=False: удаляем ВСЕ строки с запрещенными значениями
        - remove_unconditionally=True, check_by_inn=True: удаляем, НО если по ИНН есть другие значения - не удаляем
        - remove_unconditionally=True, check_by_tn=True: удаляем, НО если по ТН есть другие значения - не удаляем
        - remove_unconditionally=True, check_by_inn=True, check_by_tn=True: удаляем, НО если по ИНН ИЛИ по ТН есть другие значения - не удаляем (логика ИЛИ)
        - remove_unconditionally=False: строки НЕ удаляются, правило игнорируется
    """
    alias: str
    values: List[str]
    remove_unconditionally: bool = True
    check_by_inn: bool = False
    check_by_tn: bool = False


@dataclass
class IncludeRule:
    """
    Правило включения строк.
    
    Строка попадает в расчет только если она проходит ВСЕ условия из in_rules (логика И).
    И при этом НЕ попадает под drop_rules (исключается из DROP).
    
    Параметры:
        alias: Имя поля после маппинга (из default_columns, например "type", "tb")
        values: Список значений для проверки
        condition: "in" - значение должно быть в списке values, "not_in" - значение НЕ должно быть в списке
    
    Примеры:
        IncludeRule(alias="type", values=["Активен"], condition="in") - только строки с type="Активен"
        IncludeRule(alias="tb", values=["ЦА"], condition="not_in") - только строки где tb НЕ равно "ЦА"
    """
    alias: str
    values: List[str]
    condition: str = "in"  # "in" или "not_in"


@dataclass
class FileItem:
    """
    Элемент конфигурации для одного файла.
    
    Структура соответствует примеру из репозитория:
    - key: ключ файла (для идентификации)
    - label: подпись для логов
    - file_name: имя файла в каталоге IN (если пустое "", файл не используется)
    - sheet: название листа (если None, используется default_sheet из группы)
    - columns: список колонок (если пустой [], используются из defaults.columns)
    - filters: словарь с drop_rules и in_rules (если пустые [], используются из defaults)
    - calculation_type: тип расчета для второго листа (1, 2, 3 или None - использовать default)
    - first_month_value: значение для первого месяца при расчете типа 2 (None - использовать default)
    """
    # Ключ файла (для идентификации, например "OD_01", "OD_02")
    key: str
    
    # Подпись для логов (например "OD Январь", "OD Февраль")
    label: str
    
    # Имя файла в каталоге IN (например "OD_01.xlsx")
    # Если пустое "", файл не используется
    file_name: str
    
    # Название листа для чтения (если None, используется default_sheet из группы)
    sheet: Optional[str] = None
    
    # Колонки для этого файла (если пустой массив [], используются из defaults.columns)
    # Формат: [{"alias": "tb", "source": "Короткое ТБ"}, ...]
    columns: List[Dict[str, str]] = field(default_factory=list)
    
    # Фильтры для этого файла
    # Формат: {"drop_rules": [...], "in_rules": [...]}
    # drop_rules: список словарей {"alias": "...", "values": [...], "remove_unconditionally": True, ...}
    # in_rules: список словарей {"alias": "...", "values": [...], "condition": "in" или "not_in"}
    # Если drop_rules или in_rules пустые массивы [], используются из defaults
    filters: Dict[str, Any] = field(default_factory=dict)
    
    # Тип расчета для второго листа (1, 2, 3 или None - использовать default из группы)
    # 1: Как есть - просто сумма
    # 2: Прирост по 2 месяцам (текущий - предыдущий)
    # 3: Прирост по трем периодам (М-3 - 2*М-2 + М-1)
    calculation_type: Optional[int] = None
    
    # Значение для первого месяца при расчете типа 2
    # "self" - равен самому себе (сумме в этом месяце)
    # "zero" - равен 0
    # None - использовать default из группы
    first_month_value: Optional[str] = None
    
    # Правила для первого и второго месяца при расчете типа 3
    # "zero_both" - первый и второй месяц оба равны 0
    # "zero_first_diff_second" - первый равен 0, второй равен разнице между вторым и первым
    # "self_first_diff_second" - первый равен самому себе, второй равен разнице между вторым и первым
    # None - использовать default из группы
    three_periods_first_months: Optional[str] = None
    


@dataclass
class DefaultsConfig:
    """
    Настройки по умолчанию для группы файлов.
    
    Все эти настройки используются, если в FileItem не указаны индивидуальные значения.
    """
    # Колонки по умолчанию: маппинг source (имя в Excel) -> alias (внутреннее имя)
    columns: List[Dict[str, str]] = field(default_factory=list)
    
    # Правила удаления строк по умолчанию (drop_rules)
    drop_rules: List[DropRule] = field(default_factory=list)
    
    # Правила включения строк по умолчанию (in_rules)
    in_rules: List[IncludeRule] = field(default_factory=list)
    
    # Имена колонок после маппинга (используются alias)
    tab_number_column: str = "tab_number"
    tb_column: str = "tb"
    gosb_column: str = "gosb"
    fio_column: str = "fio"
    indicator_column: str = "indicator"
    
    # Параметры нормализации данных
    tab_number_length: int = 8  # Длина табельного номера с лидирующими нулями
    tab_number_fill_char: str = "0"  # Символ для заполнения табельного номера
    inn_length: int = 12  # Длина ИНН с лидирующими нулями
    inn_fill_char: str = "0"  # Символ для заполнения ИНН
    
    # Параметры обработки файлов
    header_row: Optional[int] = 0
    skip_rows: int = 0
    skip_footer: int = 0
    sheet_name: Optional[str] = None
    sheet_index: Optional[int] = None
    
    # Параметры расчета для второго листа
    # Тип расчета: 1 - как есть, 2 - прирост по 2 месяцам, 3 - прирост по трем периодам
    calculation_type: int = 1
    
    # Значение для первого месяца при расчете типа 2: "self" или "zero"
    first_month_value: str = "self"
    
    # Правила для первого и второго месяца при расчете типа 3
    # "zero_both", "zero_first_diff_second", "self_first_diff_second"
    three_periods_first_months: str = "zero_both"
    
    # Направление показателя для расчета лучшего месяца (вариант 3 с нормализацией)
    # "MAX" - большее значение лучше, "MIN" - меньшее значение лучше
    # Используется при нормализации показателей перед расчетом Score
    indicator_direction: str = "MAX"
    
    # Вес для расчета итогового Score (для данной группы)
    # Score (M-X) = OD_norm(M-X) * weight_OD + RA_norm(M-X) * weight_RA + PS_norm(M-X) * weight_PS
    # В каждом разделе (OD, RA, PS) задается только свой вес
    weight: float = 0.33


@dataclass
class GroupConfig:
    """Конфигурация для группы файлов (OD, RA, PS)."""
    # Название группы
    name: str
    
    # Лист по умолчанию (если у конкретного файла другой лист, задайте его в items)
    default_sheet: str = "Sheet1"
    
    # Список файлов (items) - для каждого файла указываем key, label, file_name и параметры
    # Если file_name пустое "", файл не используется
    # Если columns или filters.drop_rules пустые массивы [], используются значения из defaults
    items: List[FileItem] = field(default_factory=list)
    
    # Настройки по умолчанию для этой группы
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)


class ConfigManager:
    """Менеджер конфигурации для управления настройками загрузки файлов."""
    
    def __init__(self):
        """Инициализация менеджера конфигурации с настройками по умолчанию."""
        self.groups: Dict[str, GroupConfig] = self._create_default_configs()
    
    def _create_default_configs(self) -> Dict[str, GroupConfig]:
        """
        Создает конфигурации по умолчанию для всех групп.
        
        Returns:
            Dict[str, GroupConfig]: Словарь с конфигурациями групп
        """
        configs = {}
        
        # Конфигурация для группы OD (ОперДоход)
        configs["OD"] = GroupConfig(
            name="OD",
            default_sheet="Sheet1",
            items=[
                # Параметры расчета можно задавать для каждого файла индивидуально:
                # - calculation_type: 1, 2, 3 или None (использовать default)
                # - first_month_value: "self", "zero" или None (использовать default)
                # - three_periods_first_months: "zero_both", "zero_first_diff_second", "self_first_diff_second" или None (использовать default)
                # Примеры:
                #   FileItem(..., calculation_type=2, first_month_value="zero")  # Для этого файла тип 2, первый месяц = 0
                #   FileItem(..., calculation_type=3, three_periods_first_months="self_first_diff_second")  # Для этого файла тип 3 с особыми правилами
                # Если параметры не указаны (None), используются значения из defaults
                FileItem(key="OD_01", label="OD Январь", file_name="M-1_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_02", label="OD Февраль", file_name="M-2_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_03", label="OD Март", file_name="M-3_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_04", label="OD Апрель", file_name="M-4_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_05", label="OD Май", file_name="M-5_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_06", label="OD Июнь", file_name="M-6_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_07", label="OD Июль", file_name="M-7_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_08", label="OD Август", file_name="M-8_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_09", label="OD Сентябрь", file_name="M-9_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_10", label="OD Октябрь", file_name="M-10_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_11", label="OD Ноябрь", file_name="M-11_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="OD_12", label="OD Декабрь", file_name="M-12_OD.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
            ],
            defaults=DefaultsConfig(
                # Колонки по умолчанию: маппинг source (имя в Excel) -> alias (внутреннее имя)
                # Формат: [{"alias": "внутреннее_имя", "source": "Имя в Excel"}, ...]
                # Примеры:
                #   {"alias": "tab_number", "source": "Табельный номер"}
                #   {"alias": "tb", "source": "Короткое ТБ"}
                #   {"alias": "indicator", "source": "Факт"}
                columns=[
                    {"alias": "tab_number", "source": "Табельный номер"},
                    {"alias": "tb", "source": "Короткое ТБ"},
                    {"alias": "gosb", "source": "Полное ГОСБ"},
                    {"alias": "client_id", "source": "ИНН"},
                    {"alias": "fio", "source": "ФИО"},
                    {"alias": "indicator", "source": "Факт"}
                ],
                
                # Правила удаления строк по умолчанию (drop_rules)
                # Формат: [DropRule(alias="...", values=[...], ...), ...]
                # Параметры DropRule:
                #   - alias: имя поля после маппинга (из columns)
                #   - values: список запрещенных значений
                #   - remove_unconditionally: True - удалять всегда, False - не удалять
                #   - check_by_inn: True - не удалять, если по ИНН есть другие значения
                #   - check_by_tn: True - не удалять, если по ТН есть другие значения
                # Примеры:
                #   DropRule(alias="status", values=["Удален", "Архив"], remove_unconditionally=True, check_by_inn=False, check_by_tn=False)
                #   DropRule(alias="tb", values=["ЦА"], remove_unconditionally=True, check_by_inn=True, check_by_tn=False)
                drop_rules=[
                    # DropRule(alias="status", values=["Удален", "Архив"], remove_unconditionally=True, check_by_inn=False, check_by_tn=False),
                ],
                
                # Правила включения строк по умолчанию (in_rules)
                # Формат: [IncludeRule(alias="...", values=[...], condition="..."), ...]
                # Параметры IncludeRule:
                #   - alias: имя поля после маппинга (из columns)
                #   - values: список разрешенных значений
                #   - condition: "in" - значение должно быть в списке, "not_in" - не должно быть
                # Строка попадает в расчет только если она проходит ВСЕ условия из in_rules (И)
                # Примеры:
                #   IncludeRule(alias="type", values=["Активен"], condition="in")
                #   IncludeRule(alias="tb", values=["ЦА"], condition="not_in")
                in_rules=[
                    # IncludeRule(alias="type", values=["Активен"], condition="in"),
                ],
                
                # Имена колонок после маппинга (используются alias из columns)
                # Эти имена используются для доступа к данным после преобразования
                tab_number_column="tab_number",  # Колонка с табельным номером
                tb_column="tb",                   # Колонка с ТБ (территориальный банк)
                gosb_column="gosb",               # Колонка с ГОСБ (головной офис)
                fio_column="fio",                 # Колонка с ФИО
                indicator_column="indicator",     # Колонка с показателем (факт)
                
                # Параметры обработки файлов
                header_row=0,          # Номер строки с заголовками (0 - первая строка, None - автоматическое определение)
                skip_rows=0,          # Количество строк для пропуска в начале файла
                skip_footer=0,        # Количество строк для пропуска в конце файла
                sheet_name=None,      # Название листа для чтения (None - первый лист)
                sheet_index=None,     # Номер листа для чтения (0 - первый лист, None - использовать sheet_name)
                
                # Параметры расчета для второго листа "Расчеты"
                # Тип расчета (calculation_type):
                #   1 - "Как есть": просто загружаем сумму данных по табельному в указанный месяц (аналог первого листа)
                #   2 - "Прирост по 2 месяцам": текущий месяц - предыдущий месяц
                #      Пример: Февраль М-2 = Февраль М-2 - Январь М-1
                #      Пример: Апрель М-4 = Апрель М-4 - Март М-3
                #   3 - "Прирост по трем периодам": М-N = М-N - 2*М-(N-1) + М-(N-2)
                #      Пример: М-3 = М-3 - 2*М-2 + М-1
                #      Пример: М-4 = М-4 - 2*М-3 + М-2
                calculation_type=3,
                
                # Значение для первого месяца при расчете типа 2 (first_month_value):
                #   "self" - первый месяц равен самому себе (сумме по этому ТН в этом месяце)
                #   "zero" - первый месяц равен 0
                # Пример: если первый месяц = Январь М-1, то:
                #   "self" -> М-1 = сумма по ТН в январе
                #   "zero" -> М-1 = 0
                first_month_value="self",
                
                # Правила для первого и второго месяца при расчете типа 3 (three_periods_first_months):
                #   "zero_both" - первый и второй месяц оба равны 0
                #     Пример: М-1 = 0, М-2 = 0, М-3 = М-3 - 2*М-2 + М-1
                #   "zero_first_diff_second" - первый равен 0, второй равен разнице между вторым и первым
                #     Пример: М-1 = 0, М-2 = М-2 - М-1, М-3 = М-3 - 2*М-2 + М-1
                #   "self_first_diff_second" - первый равен самому себе, второй равен разнице между вторым и первым
                #     Пример: М-1 = М-1 (сумма), М-2 = М-2 - М-1, М-3 = М-3 - 2*М-2 + М-1
                three_periods_first_months="self_first_diff_second",
                
                # Направление показателя для расчета лучшего месяца (вариант 3 с нормализацией)
                # "MAX" - большее значение лучше, "MIN" - меньшее значение лучше
                indicator_direction="MAX",
                
                # Параметры нормализации данных
                tab_number_length=8,      # Длина табельного номера с лидирующими нулями
                tab_number_fill_char="0", # Символ для заполнения табельного номера
                inn_length=12,            # Длина ИНН с лидирующими нулями
                inn_fill_char="0"          # Символ для заполнения ИНН
            )
        )
        
        configs["RA"] = GroupConfig(
            name="RA",
            default_sheet="Sheet1",
            items=[
                FileItem(key="RA_01", label="RA Январь", file_name="M-1_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_02", label="RA Февраль", file_name="M-2_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_03", label="RA Март", file_name="M-3_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_04", label="RA Апрель", file_name="M-4_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_05", label="RA Май", file_name="M-5_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_06", label="RA Июнь", file_name="M-6_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_07", label="RA Июль", file_name="M-7_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_08", label="RA Август", file_name="M-8_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_09", label="RA Сентябрь", file_name="M-9_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_10", label="RA Октябрь", file_name="M-10_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_11", label="RA Ноябрь", file_name="M-11_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="RA_12", label="RA Декабрь", file_name="M-12_RA.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
            ],
            defaults=DefaultsConfig(
                columns=[{"alias": "tab_number", "source": "Табельный номер"}, {"alias": "tb", "source": "Короткое ТБ"}, {"alias": "gosb", "source": "Полное ГОСБ"}, {"alias": "client_id", "source": "ИНН"}, {"alias": "fio", "source": "ФИО"}, {"alias": "indicator", "source": "Факт"}],
                drop_rules=[], in_rules=[],
                tab_number_column="tab_number", tb_column="tb", gosb_column="gosb", fio_column="fio", indicator_column="indicator",
                header_row=0, skip_rows=0, skip_footer=0, sheet_name=None, sheet_index=None,
                calculation_type=2, first_month_value="self", three_periods_first_months="self_first_diff_second",
                indicator_direction="MIN", weight=0.33
            )
        )
        
        # Конфигурация для группы PS (Пассивы)
        configs["PS"] = GroupConfig(
            name="PS",
            default_sheet="Sheet1",
            items=[
                # Параметры расчета можно задавать для каждого файла индивидуально:
                # - calculation_type: 1, 2, 3 или None (использовать default)
                # - first_month_value: "self", "zero" или None (использовать default)
                # - three_periods_first_months: "zero_both", "zero_first_diff_second", "self_first_diff_second" или None (использовать default)
                # Примеры:
                #   FileItem(..., calculation_type=2, first_month_value="zero")  # Для этого файла тип 2, первый месяц = 0
                #   FileItem(..., calculation_type=3, three_periods_first_months="self_first_diff_second")  # Для этого файла тип 3 с особыми правилами
                # Если параметры не указаны (None), используются значения из defaults
                FileItem(key="PS_01", label="PS Январь", file_name="M-1_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_02", label="PS Февраль", file_name="M-2_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_03", label="PS Март", file_name="M-3_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_04", label="PS Апрель", file_name="M-4_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_05", label="PS Май", file_name="M-5_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_06", label="PS Июнь", file_name="M-6_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_07", label="PS Июль", file_name="M-7_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_08", label="PS Август", file_name="M-8_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_09", label="PS Сентябрь", file_name="M-9_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_10", label="PS Октябрь", file_name="M-10_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_11", label="PS Ноябрь", file_name="M-11_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
                FileItem(key="PS_12", label="PS Декабрь", file_name="M-12_PS.xlsx", sheet=None, columns=[], filters={"drop_rules": [], "in_rules": []}, calculation_type=None, first_month_value=None, three_periods_first_months=None),
            ],
            defaults=DefaultsConfig(
                columns=[{"alias": "tab_number", "source": "Табельный номер"}, {"alias": "tb", "source": "Короткое ТБ"}, {"alias": "gosb", "source": "Полное ГОСБ"}, {"alias": "client_id", "source": "ИНН"}, {"alias": "fio", "source": "ФИО"}, {"alias": "indicator", "source": "Факт"}],
                drop_rules=[], in_rules=[],
                tab_number_column="tab_number", tb_column="tb", gosb_column="gosb", fio_column="fio", indicator_column="indicator",
                header_row=0, skip_rows=0, skip_footer=0, sheet_name=None, sheet_index=None,
                calculation_type=2, first_month_value="self", three_periods_first_months="self_first_diff_second",
                indicator_direction="MAX", weight=0.34
            )
        )

        return configs
    
    def get_file_item(self, group_name: str, file_name: str) -> Optional[FileItem]:
        """
        Получает конфигурацию элемента файла (FileItem) по имени файла.
        
        Args:
            group_name: Название группы (OD, RA, PS)
            file_name: Имя файла
            
        Returns:
            Optional[FileItem]: Элемент конфигурации файла или None
        """
        if group_name not in self.groups:
            return None
        
        group_config = self.groups[group_name]
        
        # Ищем файл в items по file_name
        for item in group_config.items:
            if item.file_name == file_name:
                return item
        
        return None
    
    def get_config_for_file(self, group_name: str, file_name: str) -> Dict[str, Any]:
        """
        Получает конфигурацию для конкретного файла.
        
        Args:
            group_name: Название группы (OD, RA, PS)
            file_name: Имя файла
            
        Returns:
            Dict[str, Any]: Конфигурация для файла
        """
        if group_name not in self.groups:
            raise ValueError(f"Неизвестная группа: {group_name}")
        
        group_config = self.groups[group_name]
        
        # Ищем элемент файла в items
        file_item = self.get_file_item(group_name, file_name)
        
        # Получаем defaults из конфигурации группы
        defaults = group_config.defaults
        
        # Формируем итоговую конфигурацию
        # Колонки: если в item есть columns и он не пустой, используем их, иначе defaults
        if file_item and file_item.columns:
            columns = file_item.columns
        else:
            columns = defaults.columns
        
        # Правила удаления: если в item есть filters.drop_rules и он не пустой, используем их, иначе defaults
        if file_item and file_item.filters.get("drop_rules"):
            # Преобразуем словари в DropRule объекты
            drop_rules = [
                DropRule(
                    alias=rule["alias"],
                    values=rule["values"],
                    remove_unconditionally=rule.get("remove_unconditionally", True),
                    check_by_inn=rule.get("check_by_inn", False),
                    check_by_tn=rule.get("check_by_tn", False)
                ) for rule in file_item.filters["drop_rules"]
            ]
        else:
            drop_rules = defaults.drop_rules
        
        # Правила включения: если в item есть filters.in_rules и он не пустой, используем их, иначе defaults
        if file_item and file_item.filters.get("in_rules"):
            # Преобразуем словари в IncludeRule объекты
            in_rules = [
                IncludeRule(
                    alias=rule["alias"],
                    values=rule["values"],
                    condition=rule.get("condition", "in")
                ) for rule in file_item.filters["in_rules"]
            ]
        else:
            in_rules = defaults.in_rules
        
        # Лист: если в item есть sheet, используем его, иначе default_sheet группы
        sheet_name = file_item.sheet if file_item and file_item.sheet else group_config.default_sheet
        
        # Тип расчета: если в item есть calculation_type, используем его, иначе default
        calculation_type = file_item.calculation_type if file_item and file_item.calculation_type is not None else defaults.calculation_type
        
        # Значение для первого месяца: если в item есть first_month_value, используем его, иначе default
        first_month_value = file_item.first_month_value if file_item and file_item.first_month_value is not None else defaults.first_month_value
        
        # Правила для первого и второго месяца при расчете типа 3: если в item есть three_periods_first_months, используем его, иначе default
        three_periods_first_months = file_item.three_periods_first_months if file_item and file_item.three_periods_first_months is not None else defaults.three_periods_first_months
        
        # Направление показателя для расчета лучшего месяца (вариант 3): используем из defaults
        indicator_direction = defaults.indicator_direction
        
        result = {
            "columns": columns,
            "drop_rules": drop_rules,
            "in_rules": in_rules,
            "tab_number_column": defaults.tab_number_column,
            "tb_column": defaults.tb_column,
            "gosb_column": defaults.gosb_column,
            "fio_column": defaults.fio_column,
            "indicator_column": defaults.indicator_column,
            "header_row": defaults.header_row,
            "skip_rows": defaults.skip_rows,
            "skip_footer": defaults.skip_footer,
            "sheet_name": sheet_name,
            "sheet_index": defaults.sheet_index,
            "calculation_type": calculation_type,
            "first_month_value": first_month_value,
            "three_periods_first_months": three_periods_first_months,
            "indicator_direction": indicator_direction,
            "label": file_item.label if file_item else file_name
        }
        
        return result
    
    def add_file_item(self, group_name: str, file_item: FileItem) -> None:
        """
        Добавляет элемент файла в конфигурацию группы.
        
        Args:
            group_name: Название группы
            file_item: Элемент конфигурации файла
        """
        if group_name not in self.groups:
            raise ValueError(f"Неизвестная группа: {group_name}")
        
        self.groups[group_name].items.append(file_item)
    
    def get_group_config(self, group_name: str) -> GroupConfig:
        """
        Получает конфигурацию группы.
        
        Args:
            group_name: Название группы
            
        Returns:
            GroupConfig: Конфигурация группы
        """
        if group_name not in self.groups:
            raise ValueError(f"Неизвестная группа: {group_name}")
        
        return self.groups[group_name]


# Глобальный экземпляр менеджера конфигурации
config_manager = ConfigManager()


# ============================================================================
# МОДУЛЬ ЛОГИРОВАНИЯ
# ============================================================================

class Logger:
    """Класс для настройки и управления логированием."""
    
    def __init__(self, log_dir: str = LOG_DIR, level: str = LOG_LEVEL, theme: str = LOG_THEME):
        """
        Инициализация логгера.
        
        Args:
            log_dir: Директория для хранения логов
            level: Уровень логирования (INFO или DEBUG)
            theme: Тема логов (используется в имени файла)
        """
        self.log_dir = Path(log_dir)
        self.level = level.upper()
        self.theme = theme
        
        # Создаем директорию для логов, если её нет
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Настраиваем логгер
        self.logger = logging.getLogger("YEAR_SPOD_TOP_Month")
        self.logger.setLevel(getattr(logging, self.level, logging.INFO))
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Создаем файловый обработчик
        log_file = self._generate_log_filename()
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Создаем форматтер для DEBUG уровня
        debug_formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s [class: %(name)s | def: %(funcName)s]',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(debug_formatter)
        
        # Создаем консольный обработчик
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # Добавляем обработчики
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def _generate_log_filename(self) -> Path:
        """
        Генерирует имя файла лога по шаблону.
        
        Returns:
            Path: Путь к файлу лога
        """
        now = datetime.now()
        filename = f"{self.level}_{self.theme}_{now.strftime('%Y%m%d_%H%M')}.log"
        return self.log_dir / filename
    
    def info(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня INFO.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.info(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.info(message)
    
    def debug(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня DEBUG.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.debug(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.debug(message)
    
    def warning(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня WARNING.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.warning(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.warning(message)
    
    def error(self, message: str, class_name: Optional[str] = None, func_name: Optional[str] = None) -> None:
        """
        Логирует сообщение уровня ERROR.
        
        Args:
            message: Сообщение для логирования
            class_name: Имя класса (опционально)
            func_name: Имя функции (опционально)
        """
        if class_name and func_name:
            self.logger.error(f"{message} [class: {class_name} | def: {func_name}]")
        else:
            self.logger.error(message)


# ============================================================================
# МОДУЛЬ ОБРАБОТКИ ФАЙЛОВ
# ============================================================================

class FileProcessor:
    """Класс для обработки Excel файлов."""
    
    def __init__(self, input_dir: str = INPUT_DIR, logger_instance: Optional[Logger] = None):
        """
        Инициализация процессора файлов.
        
        Args:
            input_dir: Путь к каталогу с входными данными
            logger_instance: Экземпляр логгера
        """
        self.input_dir = Path(input_dir)
        self.groups = ["OD", "RA", "PS"]
        self.processed_files: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.unique_tab_numbers: Dict[str, Dict[str, Any]] = {}
        self.logger = logger_instance
        
        # Статистика обработки (собирается только если ENABLE_STATISTICS = True)
        self.statistics = {
            "files": {},  # Статистика по файлам: {group: {file_name: {initial_rows, dropped_by_rule, kept_by_rule, final_rows}}}
            "tab_selection": {},  # Статистика выбора табельных: {group: {file_name: {total_variants, selected_count}}}
            "summary": {}  # Итоговая статистика: {total_km, total_clients, by_tb: {tb: count}}
        }
    
    def load_all_files(self) -> None:
        """
        Загружает все файлы из подкаталогов OD, RA, PS.
        
        Файлы загружаются с учетом конфигурации для каждой группы.
        Используются только файлы из списка expected_files.
        """
        self.logger.info("Начало загрузки файлов", "FileProcessor", "load_all_files")
        
        # Для сводной статистики
        total_rows = 0
        all_client_ids = set()
        all_tab_numbers = set()
        
        for group in self.groups:
            group_path = self.input_dir / group
            if not group_path.exists():
                self.logger.warning(f"Каталог {group_path} не найден, пропускаем", "FileProcessor", "load_all_files")
                continue
            
            self.logger.info(f"Обработка группы {group}", "FileProcessor", "load_all_files")
            self.processed_files[group] = {}
            
            # Получаем конфигурацию группы
            group_config = config_manager.get_group_config(group)
            items = group_config.items
            defaults = group_config.defaults
            
            if not items:
                self.logger.warning(f"Список файлов (items) пуст для группы {group}", "FileProcessor", "load_all_files")
                continue
            
            self.logger.debug(f"Ожидается {len(items)} файлов в группе {group}", "FileProcessor", "load_all_files")
            
            # Загружаем только файлы из списка items
            for item in items:
                # Пропускаем файлы с пустым file_name
                if not item.file_name or item.file_name.strip() == "":
                    self.logger.debug(f"Файл с ключом {item.key} имеет пустое file_name, пропускаем", "FileProcessor", "load_all_files")
                    continue
                
                file_path = group_path / item.file_name
                
                if not file_path.exists():
                    self.logger.debug(f"Файл {item.file_name} (ключ: {item.key}, метка: {item.label}) не найден, пропускаем", "FileProcessor", "load_all_files")
                    continue
                
                try:
                    df = self._load_file(file_path, group)
                    if df is not None and not df.empty:
                        self.processed_files[group][file_path.name] = df
                        
                        # Статистика по файлу (DEBUG) - оптимизировано: используем nunique() без дополнительных преобразований
                        rows_count = len(df)
                        total_rows += rows_count
                        
                        # Получаем имена колонок из конфигурации (после маппинга используются alias)
                        tab_number_col = defaults.tab_number_column
                        client_id_col = "client_id"
                        
                        # ОПТИМИЗАЦИЯ: Подсчитываем уникальные значения быстро (nunique() уже оптимизирован)
                        unique_clients = 0
                        unique_tabs = 0
                        
                        if client_id_col in df.columns:
                            unique_clients = df[client_id_col].nunique()
                            # ОПТИМИЗАЦИЯ: Добавляем в общий набор только для сводной статистики (быстро)
                            # Используем nunique() вместо полной обработки всех значений
                            if len(all_client_ids) < 10000:  # Ограничиваем размер для производительности
                                valid_client_ids = df[client_id_col].dropna().astype(str).str.strip()
                                valid_client_ids = valid_client_ids[(valid_client_ids != 'nan') & (valid_client_ids != '')]
                                all_client_ids.update(valid_client_ids.unique())
                        
                        if tab_number_col in df.columns:
                            unique_tabs = df[tab_number_col].nunique()
                            # ОПТИМИЗАЦИЯ: Добавляем в общий набор только для сводной статистики (быстро)
                            if len(all_tab_numbers) < 10000:  # Ограничиваем размер для производительности
                                valid_tabs = df[tab_number_col].dropna().astype(str).str.strip()
                                valid_tabs = valid_tabs[(valid_tabs != 'nan') & (valid_tabs != '')]
                                all_tab_numbers.update(valid_tabs.unique())
                        
                        # Логируем статистику по файлу (DEBUG)
                        stats_parts = [f"{rows_count} строк"]
                        if unique_clients > 0:
                            stats_parts.append(f"{unique_clients} уникальных клиентов (ИНН)")
                        if unique_tabs > 0:
                            stats_parts.append(f"{unique_tabs} уникальных табельных номеров")
                        
                        self.logger.debug(f"Загружен файл {item.file_name} ({item.label}): {', '.join(stats_parts)}", "FileProcessor", "load_all_files")
                    else:
                        self.logger.warning(f"Файл {item.file_name} ({item.label}) загружен, но пуст", "FileProcessor", "load_all_files")
                except Exception as e:
                    self.logger.error(f"Ошибка при загрузке файла {item.file_name} ({item.label}): {str(e)}", "FileProcessor", "load_all_files")
        
        # Сводная статистика (INFO)
        stats_parts = [f"{total_rows} строк"]
        if len(all_client_ids) > 0:
            stats_parts.append(f"{len(all_client_ids)} уникальных клиентов (ИНН)")
        if len(all_tab_numbers) > 0:
            stats_parts.append(f"{len(all_tab_numbers)} уникальных табельных номеров")
        
        self.logger.info(f"Загрузка завершена. Обработано групп: {len(self.processed_files)}. Итого: {', '.join(stats_parts)}", "FileProcessor", "load_all_files")
    
    def _normalize_tab_number(self, value: Any, length: int, fill_char: str) -> str:
        """
        Нормализует табельный номер: преобразует в строку заданной длины с лидирующими нулями.
        
        Args:
            value: Значение табельного номера
            length: Длина строки
            fill_char: Символ для заполнения
            
        Returns:
            str: Нормализованный табельный номер
        """
        if pd.isna(value):
            return ""
        value_str = str(value).strip()
        if not value_str or value_str.lower() == 'nan':
            return ""
        # Удаляем лидирующие нули для корректной нормализации
        value_clean = value_str.lstrip('0') if value_str.lstrip('0') else '0'
        return value_clean.zfill(length)
    
    def _normalize_inn(self, value: Any, length: int, fill_char: str) -> str:
        """
        Нормализует ИНН: преобразует в строку заданной длины с лидирующими нулями.
        
        Args:
            value: Значение ИНН
            length: Длина строки
            fill_char: Символ для заполнения
            
        Returns:
            str: Нормализованный ИНН
        """
        if pd.isna(value):
            return ""
        value_str = str(value).strip()
        if not value_str or value_str.lower() == 'nan':
            return ""
        # Удаляем лидирующие нули для корректной нормализации
        value_clean = value_str.lstrip('0') if value_str.lstrip('0') else '0'
        return value_clean.zfill(length)
    
    def _load_file(self, file_path: Path, group_name: str) -> Optional[pd.DataFrame]:
        """
        Загружает один файл с применением конфигурации.
        
        Args:
            file_path: Путь к файлу
            group_name: Название группы
            
        Returns:
            Optional[pd.DataFrame]: DataFrame с данными или None при ошибке
        """
        try:
            # Получаем конфигурацию для файла
            config = config_manager.get_config_for_file(group_name, file_path.name)
            
            self.logger.debug(f"Загрузка файла {file_path.name} с конфигурацией: {config}", "FileProcessor", "_load_file")
            
            # Подготавливаем параметры для чтения Excel
            read_params = {}
            
            # Определяем engine
            if OPENPYXL_AVAILABLE:
                read_params['engine'] = 'openpyxl'
            
            # Параметры листа
            # Используем sheet_name из конфигурации (может быть из item.sheet или default_sheet)
            if config["sheet_name"]:
                read_params['sheet_name'] = config["sheet_name"]
            elif config["sheet_index"] is not None:
                read_params['sheet_name'] = config["sheet_index"]
            
            # Параметры пропуска строк
            if config["skip_rows"] > 0:
                read_params['skiprows'] = config["skip_rows"]
            
            if config["skip_footer"] > 0:
                read_params['skipfooter'] = config["skip_footer"]
            
            # Параметр заголовка
            if config["header_row"] is not None:
                read_params['header'] = config["header_row"]
            
            # Загружаем Excel файл
            try:
                df = pd.read_excel(file_path, **read_params)
            except Exception as e:
                # Если не удалось загрузить с параметрами, пробуем без них
                self.logger.warning(f"Ошибка при загрузке с параметрами, пробуем без них: {str(e)}", "FileProcessor", "_load_file")
                try:
                    df = pd.read_excel(file_path)
                except Exception as e2:
                    self.logger.error(f"Не удалось загрузить файл {file_path.name}: {str(e2)}", "FileProcessor", "_load_file")
                    return None
            
            # Собираем статистику: исходное количество строк
            if ENABLE_STATISTICS:
                initial_rows = len(df)
                if group_name not in self.statistics["files"]:
                    self.statistics["files"][group_name] = {}
                if file_path.name not in self.statistics["files"][group_name]:
                    self.statistics["files"][group_name][file_path.name] = {
                        "initial_rows": initial_rows,
                        "dropped_by_rule": {},
                        "kept_by_rule": {},
                        "final_rows": initial_rows
                    }
                else:
                    self.statistics["files"][group_name][file_path.name]["initial_rows"] = initial_rows
                    self.statistics["files"][group_name][file_path.name]["final_rows"] = initial_rows
            
            # Нормализуем названия колонок (убираем пробелы)
            df.columns = df.columns.str.strip()
            
            # Применяем маппинг колонок (source -> alias)
            if config["columns"]:
                # Формируем словарь маппинга: source -> alias
                column_maps = {col["source"]: col["alias"] for col in config["columns"]}
                
                # Проверяем наличие всех source колонок
                missing_columns = [col["source"] for col in config["columns"] if col["source"] not in df.columns]
                if missing_columns:
                    self.logger.warning(f"Отсутствующие колонки в файле {file_path.name}: {missing_columns}", "FileProcessor", "_load_file")
                
                # Переименовываем колонки из source в alias
                available_maps = {k: v for k, v in column_maps.items() if k in df.columns}
                df = df.rename(columns=available_maps)
                
                # Оставляем только нужные колонки (по alias)
                required_columns = [col["alias"] for col in config["columns"]]
                available_columns = [col for col in required_columns if col in df.columns]
                df = df[available_columns]
            
            # Применяем правила удаления строк (drop_rules)
            if config["drop_rules"]:
                df = self._apply_drop_rules(df, config["drop_rules"], file_path.name, group_name)
            
            # Применяем правила включения строк (in_rules)
            if config["in_rules"]:
                df = self._apply_in_rules(df, config["in_rules"], file_path.name, group_name)
            
            # Обновляем статистику: финальное количество строк
            if ENABLE_STATISTICS:
                if group_name in self.statistics["files"] and file_path.name in self.statistics["files"][group_name]:
                    self.statistics["files"][group_name][file_path.name]["final_rows"] = len(df)
            
            # Нормализуем табельные номера и ИНН
            group_config = config_manager.get_group_config(group_name)
            defaults = group_config.defaults
            
            # Нормализация табельных номеров
            tab_number_col = defaults.tab_number_column
            if tab_number_col in df.columns:
                df[tab_number_col] = df[tab_number_col].apply(
                    lambda x: self._normalize_tab_number(x, defaults.tab_number_length, defaults.tab_number_fill_char)
                )
            
            # Нормализация ИНН
            client_id_col = "client_id"
            if client_id_col in df.columns:
                df[client_id_col] = df[client_id_col].apply(
                    lambda x: self._normalize_inn(x, defaults.inn_length, defaults.inn_fill_char)
                )
            
            # Добавляем метаданные о файле
            df.attrs['file_name'] = file_path.name
            df.attrs['group_name'] = group_name
            df.attrs['file_path'] = str(file_path)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке файла {file_path}: {str(e)}", "FileProcessor", "_load_file")
            return None
    
    def _apply_drop_rules(self, df: pd.DataFrame, drop_rules: List[DropRule], file_name: str, group_name: str = "") -> pd.DataFrame:
        """
        Применяет правила удаления строк (drop_rules).
        
        Args:
            df: DataFrame для обработки
            drop_rules: Список правил удаления
            file_name: Имя файла для логирования
            
        Returns:
            DataFrame после применения правил
        """
        cleaned = df.copy()
        
        for rule in drop_rules:
            if rule.alias not in cleaned.columns:
                self.logger.warning(f"Колонка {rule.alias} отсутствует в файле {file_name}, пропускаем правило", "FileProcessor", "_apply_drop_rules")
                continue
            
            if not rule.remove_unconditionally:
                self.logger.debug(f"Колонка {rule.alias}: remove_unconditionally=False, строки не удаляются", "FileProcessor", "_apply_drop_rules")
                continue
            
            # Формируем множество запрещенных значений (в нижнем регистре для сравнения)
            forbidden = {str(v).strip().lower() for v in rule.values}
            
            # ОПТИМИЗАЦИЯ: Векторизация вместо apply() для ускорения в 10-50 раз
            # Обрабатываем NaN отдельно (быстро)
            mask_nan = cleaned[rule.alias].isna()
            
            # Преобразуем в строки и нормализуем (векторизованная операция)
            # ВАЖНО: astype(str) преобразует NaN в строку "nan", поэтому нужно исключить их
            col_str = cleaned[rule.alias].astype(str).str.strip().str.lower()
            
            # Исключаем строки "nan" (которые были NaN) из проверки
            mask_not_nan = col_str != 'nan'
            
            # Проверяем принадлежность к запрещенным значениям (векторизованная операция)
            mask_forbidden = col_str.isin(forbidden)
            
            # Исключаем NaN из результата (NaN не считаются запрещенными)
            mask_forbidden = mask_forbidden & mask_not_nan
            
            if not mask_forbidden.any():
                self.logger.debug(f"Колонка {rule.alias}: запрещенных значений не найдено", "FileProcessor", "_apply_drop_rules")
                continue
            
            if not rule.check_by_inn and not rule.check_by_tn:
                # Простое удаление без условий
                before = len(cleaned)
                cleaned = cleaned[~mask_forbidden]
                dropped_count = before - len(cleaned)
                self.logger.debug(f"Колонка {rule.alias}: удалено {dropped_count} строк (безусловно)", "FileProcessor", "_apply_drop_rules")
                
                # Собираем статистику
                if ENABLE_STATISTICS and group_name and file_name:
                    rule_key = f"{rule.alias}: {', '.join(map(str, rule.values))}"
                    if group_name not in self.statistics["files"]:
                        self.statistics["files"][group_name] = {}
                    if file_name not in self.statistics["files"][group_name]:
                        self.statistics["files"][group_name][file_name] = {"dropped_by_rule": {}, "kept_by_rule": {}}
                    if "dropped_by_rule" not in self.statistics["files"][group_name][file_name]:
                        self.statistics["files"][group_name][file_name]["dropped_by_rule"] = {}
                    if rule_key not in self.statistics["files"][group_name][file_name]["dropped_by_rule"]:
                        self.statistics["files"][group_name][file_name]["dropped_by_rule"][rule_key] = 0
                    self.statistics["files"][group_name][file_name]["dropped_by_rule"][rule_key] += dropped_count
            else:
                # Условное удаление (оптимизировано через векторизацию)
                rows_to_remove = mask_forbidden.copy()
                
                # ОПТИМИЗАЦИЯ: Векторизация проверки по ИНН
                if rule.check_by_inn and "client_id" in cleaned.columns:
                    # Группируем по ИНН и проверяем наличие незапрещенных значений
                    other_col_str = cleaned[rule.alias].astype(str).str.strip().str.lower()
                    other_mask_not_nan = other_col_str != 'nan'
                    other_mask_not_forbidden = ~other_col_str.isin(forbidden)
                    other_mask_keep = other_mask_not_nan & other_mask_not_forbidden
                    
                    # Для каждого ИНН проверяем, есть ли незапрещенные значения
                    grouped_by_inn = cleaned.groupby("client_id")[rule.alias].apply(
                        lambda x: (~x.astype(str).str.strip().str.lower().isin(forbidden) & (x.astype(str).str.strip().str.lower() != 'nan')).any()
                    )
                    
                    # Строки с ИНН, у которых есть незапрещенные значения, не удаляем
                    keep_by_inn = cleaned["client_id"].map(grouped_by_inn).fillna(False)
                    rows_to_remove = rows_to_remove & ~keep_by_inn
                
                # ОПТИМИЗАЦИЯ: Векторизация проверки по ТН
                if rule.check_by_tn:
                    tab_col = None
                    if "tab_number" in cleaned.columns:
                        tab_col = "tab_number"
                    elif "manager_id" in cleaned.columns:
                        tab_col = "manager_id"
                    
                    if tab_col:
                        # Группируем по ТН и проверяем наличие незапрещенных значений
                        grouped_by_tn = cleaned.groupby(tab_col)[rule.alias].apply(
                            lambda x: (~x.astype(str).str.strip().str.lower().isin(forbidden) & (x.astype(str).str.strip().str.lower() != 'nan')).any()
                        )
                        
                        # Строки с ТН, у которых есть незапрещенные значения, не удаляем
                        keep_by_tn = cleaned[tab_col].map(grouped_by_tn).fillna(False)
                        rows_to_remove = rows_to_remove & ~keep_by_tn
                
                before = len(cleaned)
                cleaned = cleaned[~rows_to_remove]
                dropped_count = before - len(cleaned)
                self.logger.debug(
                    f"Колонка {rule.alias}: удалено {dropped_count} строк "
                    f"(условно: remove_unconditionally={rule.remove_unconditionally}, "
                    f"check_by_inn={rule.check_by_inn}, check_by_tn={rule.check_by_tn})",
                    "FileProcessor", "_apply_drop_rules"
                )
                
                # Собираем статистику
                if ENABLE_STATISTICS and group_name and file_name:
                    rule_key = f"{rule.alias}: {', '.join(map(str, rule.values))} [условно: check_by_inn={rule.check_by_inn}, check_by_tn={rule.check_by_tn}]"
                    if group_name not in self.statistics["files"]:
                        self.statistics["files"][group_name] = {}
                    if file_name not in self.statistics["files"][group_name]:
                        self.statistics["files"][group_name][file_name] = {"dropped_by_rule": {}, "kept_by_rule": {}}
                    if "dropped_by_rule" not in self.statistics["files"][group_name][file_name]:
                        self.statistics["files"][group_name][file_name]["dropped_by_rule"] = {}
                    if rule_key not in self.statistics["files"][group_name][file_name]["dropped_by_rule"]:
                        self.statistics["files"][group_name][file_name]["dropped_by_rule"][rule_key] = 0
                    self.statistics["files"][group_name][file_name]["dropped_by_rule"][rule_key] += dropped_count
        
        return cleaned
    
    def _apply_in_rules(self, df: pd.DataFrame, in_rules: List[IncludeRule], file_name: str, group_name: str = "") -> pd.DataFrame:
        """
        Применяет правила включения строк (in_rules).
        
        Строка попадает в расчет только если она проходит ВСЕ условия из in_rules (И).
        
        Args:
            df: DataFrame для обработки
            in_rules: Список правил включения
            file_name: Имя файла для логирования
            
        Returns:
            DataFrame после применения правил
        """
        if not in_rules:
            return df
        
        # Начинаем с маски True для всех строк
        final_mask = pd.Series(True, index=df.index)
        
        for rule in in_rules:
            if rule.alias not in df.columns:
                self.logger.warning(f"Колонка {rule.alias} отсутствует в файле {file_name}, пропускаем правило", "FileProcessor", "_apply_in_rules")
                continue
            
            # Формируем множество разрешенных значений
            allowed = {str(v).strip().lower() for v in rule.values}
            
            def check_value(value: Any) -> bool:
                """Проверяет значение по условию."""
                if pd.isna(value):
                    return False
                value_str = str(value).strip().lower()
                if rule.condition == "in":
                    return value_str in allowed
                elif rule.condition == "not_in":
                    return value_str not in allowed
                return False
            
            # Применяем условие (И - все условия должны выполняться)
            rule_mask = df[rule.alias].apply(check_value)
            final_mask = final_mask & rule_mask
        
        before = len(df)
        result = df[final_mask]
        kept_count = len(result)
        dropped_count = before - kept_count
        self.logger.debug(f"После применения in_rules: оставлено {kept_count} строк из {before}", "FileProcessor", "_apply_in_rules")
        
        # Собираем статистику
        if ENABLE_STATISTICS and group_name and file_name and in_rules:
            if group_name not in self.statistics["files"]:
                self.statistics["files"][group_name] = {}
            if file_name not in self.statistics["files"][group_name]:
                self.statistics["files"][group_name][file_name] = {"dropped_by_rule": {}, "kept_by_rule": {}}
            if "kept_by_rule" not in self.statistics["files"][group_name][file_name]:
                self.statistics["files"][group_name][file_name]["kept_by_rule"] = {}
            
            for rule in in_rules:
                rule_key = f"{rule.alias}: {rule.condition} {', '.join(map(str, rule.values))}"
                if rule_key not in self.statistics["files"][group_name][file_name]["kept_by_rule"]:
                    self.statistics["files"][group_name][file_name]["kept_by_rule"][rule_key] = 0
                # Приблизительная оценка: считаем, что все правила вносят равный вклад
                self.statistics["files"][group_name][file_name]["kept_by_rule"][rule_key] = kept_count
        
        return result
    
    def collect_unique_tab_numbers(self) -> None:
        """
        Собирает уникальные табельные номера из всех файлов.
        
        Алгоритм:
        1. В каждом файле табельные номера должны быть уникальны (если есть дубликаты, берется первая строка)
        2. Поиск выполняется в порядке приоритета:
           - Группы: OD -> RA -> PS
           - Месяцы: декабрь (M-12) -> ноябрь (M-11) -> ... -> январь (M-1)
        3. Для каждого табельного номера берется ПЕРВЫЙ найденный ТБ и ГОСБ
        4. Если табельный номер уже найден в файле с более высоким приоритетом, 
           он НЕ обновляется - остается ранее найденный ТБ и ГОСБ
        
        Результат: каждый табельный номер встречается в итоговом списке только один раз.
        """
        self.logger.info("Начало сбора уникальных табельных номеров", "FileProcessor", "collect_unique_tab_numbers")
        
        # Порядок приоритета групп
        group_priority = {"OD": 1, "RA": 2, "PS": 3}

        # ОПТИМИЗАЦИЯ: Кэш для номеров месяцев
        month_cache = {}
        
        # Извлекаем номер месяца из имени файла
        def extract_month_number(file_name: str) -> int:
            """
            Извлекает номер месяца из имени файла.

            Поддерживает форматы:
            - M-{номер}_{группа}.xlsx (например, M-1_RA.xlsx, M-12_OD.xlsx)
            - {группа}_{номер}.xlsx (например, RA_01.xlsx, OD_12.xlsx)
            - T-{номер} (например, T-11, T-0) - где T-11 = январь, T-0 = декабрь

            Args:
                file_name: Имя файла
                
            Returns:
                int: Номер месяца (1-12) или 0, если не удалось определить
            """
            # ОПТИМИЗАЦИЯ: Проверяем кэш
            if file_name in month_cache:
                return month_cache[file_name]
            
            # Паттерн для формата M-{номер}_{группа}.xlsx
            match = re.search(r'M-(\d{1,2})_', file_name)
            if match:
                month = int(match.group(1))
                if 1 <= month <= 12:
                    month_cache[file_name] = month
                    return month
            
            # Паттерн для формата {группа}_{номер}.xlsx (например, RA_01.xlsx)
            match = re.search(r'_(\d{2})\.', file_name)
            if match:
                month = int(match.group(1))
                if 1 <= month <= 12:
                    month_cache[file_name] = month
                    return month
            
            # Паттерн для формата T-{номер} (T-11 = январь, T-0 = декабрь)
            match = re.search(r'T-(\d{1,2})', file_name)
            if match:
                t_value = int(match.group(1))
                # Преобразуем T-11 -> 1 (январь), T-0 -> 12 (декабрь)
                if 0 <= t_value <= 11:
                    month = 12 - t_value
                    if 1 <= month <= 12:
                        month_cache[file_name] = month
                        return month
            
            # Если не нашли, возвращаем 0 (низкий приоритет)
            result = 0
            month_cache[file_name] = result
            return result
        
        # Собираем все табельные номера с информацией о файлах
        all_tab_data: Dict[str, Dict[str, Any]] = {}
        
        # Проходим по группам в порядке приоритета
        for group in sorted(self.groups, key=lambda x: group_priority.get(x, 999)):
            if group not in self.processed_files:
                continue
            
            group_config = config_manager.get_group_config(group)
            defaults = group_config.defaults
            tab_col = defaults.tab_number_column
            tb_col = defaults.tb_column
            gosb_col = defaults.gosb_column
            fio_col = defaults.fio_column
            
            # Сортируем файлы по номеру месяца (от большего к меньшему)
            files_sorted = sorted(
                self.processed_files[group].items(),
                key=lambda x: extract_month_number(x[0]),
                reverse=True
            )
            
            for file_name, df in files_sorted:
                month = extract_month_number(file_name)
                self.logger.debug(f"Обработка файла {file_name} группы {group}, месяц {month}", "FileProcessor", "collect_unique_tab_numbers")
                
                if tab_col not in df.columns:
                    self.logger.warning(f"Колонка '{tab_col}' не найдена в файле {file_name}", "FileProcessor", "collect_unique_tab_numbers")
                    continue
                
                # Табельные номера уже нормализованы при загрузке файла
                df_normalized = df.copy()
                # Фильтруем пустые и некорректные значения
                df_normalized = df_normalized[df_normalized[tab_col].notna()]
                df_normalized = df_normalized[df_normalized[tab_col] != '']
                
                if len(df_normalized) == 0:
                    continue
                
                # ВАЖНО: В каждом файле табельные номера должны быть уникальны
                # Если у табельного номера несколько разных ТБ/ГОСБ, выбираем тот, у которого сумма показателя больше
                # Это делается только если табельный номер еще не встречался ранее
                current_priority = group_priority[group] * 100 + month
                indicator_col = defaults.indicator_column
                
                # ОПТИМИЗАЦИЯ: Выбираем уникальные строки для каждого табельного номера
                # Сначала суммируем показатели по комбинациям ТН+ТБ+ГОСБ, затем выбираем максимум
                if indicator_col in df_normalized.columns:
                    # Шаг 1: Группируем по ТН+ТБ+ГОСБ и суммируем показатели (быстро, векторизовано)
                    group_cols = [tab_col]
                    if tb_col in df_normalized.columns:
                        group_cols.append(tb_col)
                    if gosb_col in df_normalized.columns:
                        group_cols.append(gosb_col)
                    
                    grouped = df_normalized.groupby(group_cols, as_index=False)[indicator_col].sum()
                    
                    # Шаг 2: Для каждого ТН находим строку с максимальной суммой (векторизовано)
                    # Используем groupby().idxmax() - это векторизованная операция, заменяет цикл
                    max_indices = grouped.groupby(tab_col)[indicator_col].idxmax()
                    max_rows = grouped.loc[max_indices]
                    
                    # Собираем статистику по выбору табельных номеров
                    if ENABLE_STATISTICS:
                        if group not in self.statistics["tab_selection"]:
                            self.statistics["tab_selection"][group] = {}
                        if file_name not in self.statistics["tab_selection"][group]:
                            self.statistics["tab_selection"][group][file_name] = {
                                "total_variants": 0,
                                "selected_count": 0,
                                "variants_with_multiple": 0
                            }
                        
                        # Подсчитываем количество вариантов для каждого табельного
                        for tab_num in grouped[tab_col].unique():
                            tab_data = grouped[grouped[tab_col] == tab_num]
                            variant_count = len(tab_data)
                            self.statistics["tab_selection"][group][file_name]["total_variants"] += variant_count
                            if variant_count > 1:
                                self.statistics["tab_selection"][group][file_name]["variants_with_multiple"] += 1
                        
                        self.statistics["tab_selection"][group][file_name]["selected_count"] = len(max_rows)
                    
                    # Логируем выбор для отладки
                    for _, max_row in max_rows.iterrows():
                        tab_num = max_row[tab_col]
                        tab_data = grouped[grouped[tab_col] == tab_num]
                        if len(tab_data) > 1:
                            self.logger.debug(f"В файле {file_name} для табельного {tab_num} найдено {len(tab_data)} вариантов ТБ/ГОСБ, выбран вариант с максимальной суммой показателя: {max_row[indicator_col]:.2f}", "FileProcessor", "collect_unique_tab_numbers")
                    
                    # Шаг 3: Находим соответствующие строки в исходном DataFrame через merge (быстро)
                    # Используем merge вместо циклов с mask - это векторизованная операция
                    df_unique = df_normalized.merge(
                        max_rows[[tab_col, tb_col, gosb_col]],
                        on=[tab_col, tb_col, gosb_col],
                        how='inner'
                    ).drop_duplicates(subset=[tab_col], keep='first')
                else:
                    # Если нет колонки с показателем, используем старую логику
                    df_unique = df_normalized.drop_duplicates(subset=[tab_col], keep='first')
                    
                    # Собираем статистику для случая без показателя
                    if ENABLE_STATISTICS:
                        if group not in self.statistics["tab_selection"]:
                            self.statistics["tab_selection"][group] = {}
                        if file_name not in self.statistics["tab_selection"][group]:
                            self.statistics["tab_selection"][group][file_name] = {
                                "total_variants": len(df_normalized),
                                "selected_count": len(df_unique),
                                "variants_with_multiple": 0
                            }
                        else:
                            self.statistics["tab_selection"][group][file_name]["total_variants"] = len(df_normalized)
                            self.statistics["tab_selection"][group][file_name]["selected_count"] = len(df_unique)
                
                if len(df_unique) < len(df_normalized):
                    duplicates_count = len(df_normalized) - len(df_unique)
                    self.logger.debug(f"В файле {file_name} найдено {duplicates_count} дубликатов табельных номеров, оставлено уникальных: {len(df_unique)}", "FileProcessor", "collect_unique_tab_numbers")
                
                # ВАЖНО: Используем нормализованные значения из df_unique напрямую
                for idx in df_unique.index:
                    # Получаем нормализованные значения из df_unique (уже нормализованы при загрузке)
                    tab_number = str(df_unique.loc[idx, tab_col])
                    
                    if not tab_number or tab_number == '':
                        continue
                    
                    # ВАЖНО: Если табельный номер уже найден ранее (в файле с более высоким приоритетом),
                    # НЕ обновляем его - оставляем ранее найденный ТБ и ГОСБ
                    # Алгоритм: ищем от OD к PS, от декабря к январю, берем ПЕРВЫЙ найденный
                    if tab_number not in all_tab_data:
                        # Табельный номер еще не встречался - добавляем его
                        all_tab_data[tab_number] = {
                            "tab_number": tab_number,
                            "tb": str(df_unique.loc[idx, tb_col]).strip() if tb_col in df_unique.columns else "",
                            "gosb": str(df_unique.loc[idx, gosb_col]).strip() if gosb_col in df_unique.columns else "",
                            "fio": str(df_unique.loc[idx, fio_col]).strip() if fio_col in df_unique.columns else "",
                            "group": group,
                            "month": month,
                            "priority": current_priority
                        }
                    # Если табельный номер уже найден, НЕ обновляем - оставляем ранее найденный
        
        self.unique_tab_numbers = all_tab_data
        
        # ВАЖНО: Проверяем на дубликаты (должно быть уникально)
        if len(all_tab_data) != len(set(all_tab_data.keys())):
            self.logger.warning(f"Обнаружены дубликаты табельных номеров в all_tab_data! Всего ключей: {len(all_tab_data)}, уникальных: {len(set(all_tab_data.keys()))}", "FileProcessor", "collect_unique_tab_numbers")
        
        # Логируем статистику по группам и месяцам
        group_stats = {}
        for tab_number, data in all_tab_data.items():
            group = data["group"]
            month = data["month"]
            key = f"{group}_M-{month}"
            if key not in group_stats:
                group_stats[key] = 0
            group_stats[key] += 1
        
        self.logger.debug(f"Распределение табельных номеров по группам и месяцам: {group_stats}", "FileProcessor", "collect_unique_tab_numbers")
        self.logger.info(f"Собрано {len(self.unique_tab_numbers)} уникальных табельных номеров", "FileProcessor", "collect_unique_tab_numbers")
    
    def prepare_summary_data(self) -> pd.DataFrame:
        """
        Подготавливает сводные данные для итогового файла.
        
        Для каждого табельного номера собирает суммы показателей из каждого файла.
        
        Returns:
            pd.DataFrame: DataFrame со сводными данными
        """
        self.logger.info("=== Начало подготовки сводных данных для листа 'Данные' ===", "FileProcessor", "prepare_summary_data")
        
        if not self.unique_tab_numbers:
            self.logger.warning("Уникальные табельные номера не собраны", "FileProcessor", "prepare_summary_data")
            self.collect_unique_tab_numbers()
        
        # ОПТИМИЗАЦИЯ: Кэш для номеров месяцев
        month_cache = {}
        
        # Извлекаем номер месяца из имени файла для сортировки
        def extract_month_number(file_name: str) -> int:
            """Извлекает номер месяца из имени файла."""
            if file_name in month_cache:
                return month_cache[file_name]
            match = re.search(r'M-(\d{1,2})_', file_name)
            if match:
                month = int(match.group(1))
                if 1 <= month <= 12:
                    month_cache[file_name] = month
                    return month
            month_cache[file_name] = 0
            return 0
        
        # Создаем список всех файлов в порядке обработки
        # Порядок: для каждой группы (OD, RA, PS) файлы сортируются по месяцам (M-1, M-2, ..., M-12)
        all_files: List[Tuple[str, str, str]] = []  # (group, file_name, full_name)
        
        # Логируем информацию о группах и месяцах (DEBUG - детальная информация)
        for group in self.groups:
            if group in self.processed_files:
                # Сортируем файлы по номеру месяца (1-12)
                files_sorted = sorted(
                    self.processed_files[group].keys(),
                    key=lambda x: extract_month_number(x)
                )
                months_list = [extract_month_number(fn) for fn in files_sorted]
                self.logger.debug(f"Лист 'Данные': Группа {group}, обрабатываем месяцы: {months_list} (M-{min(months_list)} ... M-{max(months_list)})", "FileProcessor", "prepare_summary_data")
                for file_name in files_sorted:
                    full_name = f"{group}_{file_name}"
                    all_files.append((group, file_name, full_name))
        
        self.logger.debug(f"Лист 'Данные': Всего колонок для обработки: {len(all_files)} (базовые: Табельный, ТБ, ГОСБ, ФИО + данные по группам и месяцам)", "FileProcessor", "prepare_summary_data")
        
        # ОПТИМИЗАЦИЯ: Предварительно создаем индексы для всех файлов
        # Кэшируем конфигурации групп
        self.logger.debug("Лист 'Данные': Создание индексов по табельным номерам для всех файлов", "FileProcessor", "prepare_summary_data")
        file_indexes = {}  # {full_name: {tab_number: sum}}
        group_configs_cache = {}  # Кэш конфигураций
        
        for group, file_name, full_name in all_files:
            if group in self.processed_files and file_name in self.processed_files[group]:
                df = self.processed_files[group][file_name]
                
                # Кэшируем конфигурацию группы
                if group not in group_configs_cache:
                    group_configs_cache[group] = config_manager.get_group_config(group)
                
                defaults = group_configs_cache[group].defaults
                tab_col = defaults.tab_number_column
                indicator_col = defaults.indicator_column
                
                if tab_col not in df.columns or indicator_col not in df.columns:
                    file_indexes[full_name] = {}
                    continue
                
                # ОПТИМИЗАЦИЯ: Нормализуем табельные номера один раз
                df_normalized = df.copy()
                df_normalized[tab_col] = df_normalized[tab_col].astype(str).str.strip()
                df_normalized = df_normalized[df_normalized[tab_col] != 'nan']
                df_normalized = df_normalized[df_normalized[tab_col] != '']
                
                # ОПТИМИЗАЦИЯ: Группируем по табельным номерам и суммируем показатели один раз для всего файла
                grouped = df_normalized.groupby(tab_col)[indicator_col].sum()
                file_indexes[full_name] = grouped.to_dict()
        
        self.logger.debug(f"Лист 'Данные': Индексы созданы для {len(file_indexes)} файлов", "FileProcessor", "prepare_summary_data")
        
        # Создаем структуру данных
        result_data = []
        total_tab_numbers = len(self.unique_tab_numbers)
        self.logger.info(f"Лист 'Данные': Обработка {total_tab_numbers} уникальных табельных номеров", "FileProcessor", "prepare_summary_data")
        
        processed_count = 0
        for tab_number, tab_info in self.unique_tab_numbers.items():
            processed_count += 1
            # Логируем прогресс каждые 100 записей или в начале/конце (DEBUG - детальная информация)
            if processed_count == 1 or processed_count % 100 == 0 or processed_count == total_tab_numbers:
                self.logger.debug(f"Лист 'Данные': Обработано {processed_count} из {total_tab_numbers} табельных номеров ({processed_count * 100 // total_tab_numbers if total_tab_numbers > 0 else 0}%)", "FileProcessor", "prepare_summary_data")
            # Форматируем табельный номер: 8 знаков с лидирующими нулями
            tab_number_formatted = str(tab_number).zfill(8) if tab_number else "00000000"
            
            row = {
                "Табельный": tab_number_formatted,
                "ТБ": tab_info["tb"],
                "ГОСБ": tab_info["gosb"],
                "ФИО": tab_info["fio"]
            }
            
            # ОПТИМИЗАЦИЯ: Используем предварительно созданные индексы вместо фильтрации
            for group, file_name, full_name in all_files:
                if full_name in file_indexes:
                    row[full_name] = file_indexes[full_name].get(tab_number, 0)
                else:
                    row[full_name] = 0
            
            result_data.append(row)
        
        self.logger.debug(f"Лист 'Данные': Завершена обработка всех табельных номеров, формирование DataFrame из {len(result_data)} строк", "FileProcessor", "prepare_summary_data")
        result_df = pd.DataFrame(result_data)
        self.logger.debug(f"Лист 'Данные': DataFrame создан, размер: {len(result_df)} строк x {len(result_df.columns)} колонок", "FileProcessor", "prepare_summary_data")
        
        # Собираем итоговую статистику
        if ENABLE_STATISTICS:
            # Количество КМ (табельных номеров)
            self.statistics["summary"]["total_km"] = len(result_df)
            
            # Количество уникальных клиентов
            if "ID_Clients" in result_df.columns:
                unique_clients = result_df["ID_Clients"].nunique()
                self.statistics["summary"]["total_clients"] = unique_clients
            
            # Количество КМ по ТБ
            if "ТБ" in result_df.columns:
                by_tb = result_df["ТБ"].value_counts().to_dict()
                self.statistics["summary"]["by_tb"] = by_tb
            
            # Количество КМ по ГОСБ
            if "ГОСБ" in result_df.columns:
                by_gosb = result_df["ГОСБ"].value_counts().to_dict()
                self.statistics["summary"]["by_gosb"] = by_gosb
        
        # Нормализуем табельные номера и ИНН в выходных данных
        # Получаем параметры нормализации из первой группы (все группы должны иметь одинаковые параметры)
        first_group = list(config_manager.groups.keys())[0] if config_manager.groups else None
        if first_group:
            defaults = config_manager.get_group_config(first_group).defaults
            if "Табельный" in result_df.columns:
                result_df["Табельный"] = result_df["Табельный"].apply(
                    lambda x: self._normalize_tab_number(x, defaults.tab_number_length, defaults.tab_number_fill_char)
                )
            if "ID_Clients" in result_df.columns:
                result_df["ID_Clients"] = result_df["ID_Clients"].apply(
                    lambda x: self._normalize_inn(x, defaults.inn_length, defaults.inn_fill_char)
                )

        # ВАЖНО: Проверяем на дубликаты табельных номеров в итоговом результате
        if "Табельный" in result_df.columns:
            duplicates = result_df[result_df.duplicated(subset=["Табельный"], keep=False)]
            if len(duplicates) > 0:
                duplicate_tabs = duplicates["Табельный"].unique()
                self.logger.warning(f"Лист 'Данные': Обнаружено {len(duplicate_tabs)} дубликатов табельных номеров в итоговом результате! Примеры: {list(duplicate_tabs[:5])}", "FileProcessor", "prepare_summary_data")
                # Удаляем дубликаты, оставляя первую запись
                result_df = result_df.drop_duplicates(subset=["Табельный"], keep='first')
                self.logger.warning(f"Лист 'Данные': Дубликаты удалены, осталось {len(result_df)} уникальных табельных номеров", "FileProcessor", "prepare_summary_data")
        
        # Упорядочиваем колонки: сначала базовые, потом по группам и месяцам
        self.logger.debug("Лист 'Данные': Упорядочивание колонок", "FileProcessor", "prepare_summary_data")
        base_columns = ["Табельный", "ТБ", "ГОСБ", "ФИО"]
        data_columns = [full_name for _, _, full_name in all_files]
        ordered_columns = base_columns + data_columns
        
        # Оставляем только существующие колонки
        existing_columns = [col for col in ordered_columns if col in result_df.columns]
        # Добавляем колонки, которых нет в списке (на случай если что-то пропущено)
        other_columns = [col for col in result_df.columns if col not in existing_columns]
        final_columns = existing_columns + other_columns
        
        result_df = result_df[final_columns]
        self.logger.debug(f"Лист 'Данные': Колонки упорядочены, итоговое количество: {len(result_df.columns)}", "FileProcessor", "prepare_summary_data")
        self.logger.info(f"Лист 'Данные': Подготовлено {len(result_df)} строк сводных данных, колонок: {len(result_df.columns)}", "FileProcessor", "prepare_summary_data")
        self.logger.info("=== Завершена подготовка сводных данных для листа 'Данные' ===", "FileProcessor", "prepare_summary_data")
        
        return result_df
    
    def prepare_calculated_data(self, summary_df: pd.DataFrame) -> pd.DataFrame:
        """
        Подготавливает данные с расчетами для второго листа.
        
        Варианты расчета:
        1: Как есть - просто сумма
        2: Прирост по 2 месяцам (текущий - предыдущий)
        3: Прирост по трем периодам (М-3 - 2*М-2 + М-1)
        
        Args:
            summary_df: DataFrame с исходными данными из prepare_summary_data
            
        Returns:
            pd.DataFrame: DataFrame с расчетными данными
        """
        self.logger.info("=== Начало подготовки расчетных данных для листа 'Расчеты' ===", "FileProcessor", "prepare_calculated_data")
        
        # ОПТИМИЗАЦИЯ: Кэш для номеров месяцев
        month_cache = {}
        
        # Извлекаем номер месяца из имени файла
        def extract_month_number(file_name: str) -> int:
            """Извлекает номер месяца из имени файла."""
            if file_name in month_cache:
                return month_cache[file_name]
            match = re.search(r'M-(\d{1,2})_', file_name)
            if match:
                month = int(match.group(1))
                if 1 <= month <= 12:
                    month_cache[file_name] = month
                    return month
            month_cache[file_name] = 0
            return 0
        
        # Функция для генерации понятного имени колонки на основе типа расчета
        def generate_column_name(group: str, month: int, calc_type: int, 
                                 prev_month: Optional[int] = None, 
                                 prev2_month: Optional[int] = None) -> str:
            """
            Генерирует понятное имя колонки в формате: OD (M-1) [как считалось]
            
            Args:
                group: Название группы (OD, RA, PS)
                month: Номер текущего месяца
                calc_type: Тип расчета (1, 2, 3)
                prev_month: Номер предыдущего месяца (для типа 2 и 3)
                prev2_month: Номер пред-предыдущего месяца (для типа 3)
                
            Returns:
                str: Понятное имя колонки в формате "OD (M-1) [описание расчета]"
            """
            month_str = f"M-{month}"
            period_part = f"{group} ({month_str})"
            
            if calc_type == 1:
                # Тип 1: Как есть - просто факт
                return f"{period_part} [факт]"
            elif calc_type == 2:
                # Тип 2: Прирост по 2 месяцам
                if prev_month is not None:
                    prev_month_str = f"M-{prev_month}"
                    return f"{period_part} [{month_str}→{prev_month_str}]"
                else:
                    # Первый месяц
                    return f"{period_part} [факт]"
            elif calc_type == 3:
                # Тип 3: Прирост по трем периодам
                if prev_month is not None and prev2_month is not None:
                    prev_month_str = f"M-{prev_month}"
                    prev2_month_str = f"M-{prev2_month}"
                    return f"{period_part} [{month_str}-2*{prev_month_str}+{prev2_month_str}]"
                elif prev_month is not None:
                    # Второй месяц
                    prev_month_str = f"M-{prev_month}"
                    return f"{period_part} [{month_str}→{prev_month_str}]"
                else:
                    # Первый месяц
                    return f"{period_part} [факт]"
            else:
                return f"{period_part} [факт]"
        
        # Создаем копию базовых данных
        calculated_df = summary_df.copy()
        
        # Базовые колонки
        base_columns = ["Табельный", "ТБ", "ГОСБ", "ФИО"]
        
        # Словарь для переименования колонок
        rename_dict = {}
        
        # Получаем список всех файлов в порядке обработки
        all_files: List[Tuple[str, str, str, int]] = []  # (group, file_name, full_name, month)
        
        for group in self.groups:
            if group in self.processed_files:
                files_sorted = sorted(
                    self.processed_files[group].keys(),
                    key=lambda x: extract_month_number(x)
                )
                for file_name in files_sorted:
                    month = extract_month_number(file_name)
                    full_name = f"{group}_{file_name}"
                    all_files.append((group, file_name, full_name, month))
        
        # Сортируем по группе и месяцу
        all_files_sorted = sorted(all_files, key=lambda x: (x[0], x[3]))
        
        # Для каждой группы обрабатываем файлы по порядку
        for group in self.groups:
            group_files = [(g, fn, fname, m) for g, fn, fname, m in all_files_sorted if g == group]
            if not group_files:
                continue
            
            self.logger.debug(f"Лист 'Расчеты': Обработка группы {group}, файлов: {len(group_files)}", "FileProcessor", "prepare_calculated_data")
            group_config = config_manager.get_group_config(group)
            
            for idx, (g, file_name, full_name, month) in enumerate(group_files):
                if full_name not in calculated_df.columns:
                    continue
                
                # Получаем конфигурацию для файла
                file_config = config_manager.get_config_for_file(group, file_name)
                calc_type = file_config.get("calculation_type", 1)
                first_month_val = file_config.get("first_month_value", "self")
                three_periods_mode = file_config.get("three_periods_first_months", "zero_both")
                
                # Определяем предыдущие месяцы для генерации имени
                prev_month = None
                prev2_month = None
                
                if calc_type == 2 and idx > 0:
                    prev_month = group_files[idx - 1][3]
                elif calc_type == 3:
                    if idx > 0:
                        prev_month = group_files[idx - 1][3]
                    if idx > 1:
                        prev2_month = group_files[idx - 2][3]
                
                # Генерируем понятное имя колонки
                new_name = generate_column_name(group, month, calc_type, prev_month, prev2_month)
                rename_dict[full_name] = new_name
                
                # Логируем информацию о типе расчета
                calc_type_names = {1: "Как есть (факт)", 2: "Прирост по 2 месяцам", 3: "Прирост по трем периодам"}
                calc_desc = calc_type_names.get(calc_type, f"Тип {calc_type}")
                if calc_type == 2:
                    if idx == 0:
                        calc_desc += f", первый месяц: {first_month_val}"
                    else:
                        calc_desc += f", M-{month} - M-{prev_month}"
                elif calc_type == 3:
                    if idx == 0:
                        calc_desc += f", режим: {three_periods_mode}"
                    elif idx == 1:
                        calc_desc += f", режим: {three_periods_mode}, M-{month} - M-{prev_month}"
                    else:
                        calc_desc += f", M-{month} - 2*M-{prev_month} + M-{prev2_month}"
                
                self.logger.debug(f"Лист 'Расчеты': Группа {group}, месяц M-{month}, тип расчета: {calc_desc}, колонка: {new_name}", "FileProcessor", "prepare_calculated_data")
                
                if calc_type == 1:
                    # Вариант 1: Как есть - просто копируем значение
                    calculated_df[full_name] = summary_df[full_name]
                
                elif calc_type == 2:
                    # Вариант 2: Прирост по 2 месяцам
                    if idx == 0:
                        # Первый месяц
                        if first_month_val == "self":
                            calculated_df[full_name] = summary_df[full_name]
                        else:  # "zero"
                            calculated_df[full_name] = 0
                    else:
                        # Текущий месяц минус предыдущий
                        prev_file_name = group_files[idx - 1][2]
                        if prev_file_name in summary_df.columns:
                            calculated_df[full_name] = summary_df[full_name] - summary_df[prev_file_name]
                        else:
                            calculated_df[full_name] = summary_df[full_name]
                
                elif calc_type == 3:
                    # Вариант 3: Прирост по трем периодам (М-3 - 2*М-2 + М-1)
                    if idx == 0:
                        # Первый месяц
                        if three_periods_mode == "self_first_diff_second":
                            calculated_df[full_name] = summary_df[full_name]
                        else:  # "zero_both" или "zero_first_diff_second"
                            calculated_df[full_name] = 0
                    elif idx == 1:
                        # Второй месяц
                        if three_periods_mode == "zero_both":
                            calculated_df[full_name] = 0
                        else:  # "zero_first_diff_second" или "self_first_diff_second"
                            prev_file_name = group_files[0][2]
                            if prev_file_name in summary_df.columns:
                                calculated_df[full_name] = summary_df[full_name] - summary_df[prev_file_name]
                            else:
                                calculated_df[full_name] = summary_df[full_name]
                    else:
                        # М-3 - 2*М-2 + М-1
                        curr_val = summary_df[full_name]
                        prev1_file_name = group_files[idx - 1][2]
                        prev2_file_name = group_files[idx - 2][2]
                        
                        prev1_val = summary_df[prev1_file_name] if prev1_file_name in summary_df.columns else 0
                        prev2_val = summary_df[prev2_file_name] if prev2_file_name in summary_df.columns else 0
                        
                        calculated_df[full_name] = curr_val - 2 * prev1_val + prev2_val
        
        # Переименовываем колонки на понятные имена (только те, которые существуют в DataFrame)
        existing_rename_dict = {k: v for k, v in rename_dict.items() if k in calculated_df.columns}
        calculated_df = calculated_df.rename(columns=existing_rename_dict)
        self.logger.debug(f"Лист 'Расчеты': Переименовано колонок: {len(existing_rename_dict)}", "FileProcessor", "prepare_calculated_data")
        
        # НЕ рассчитываем вертикальные ранги (убрано для варианта 3)
        # calculated_df = self._calculate_ranks(calculated_df, all_files_sorted, config_manager)

        self.logger.info(f"Лист 'Расчет': Подготовлено {len(calculated_df)} строк расчетных данных, колонок: {len(calculated_df.columns)}", "FileProcessor", "prepare_calculated_data")
        self.logger.info("=== Завершена подготовка расчетных данных для листа 'Расчет' ===", "FileProcessor", "prepare_calculated_data")

        return calculated_df
    
    def _normalize_indicators(self, calculated_df: pd.DataFrame, config_manager) -> pd.DataFrame:
        """
        Нормализует показатели для каждого КМ по месяцам с учетом направления (вариант 3).
        
        Создает лист "Нормализация" с нормализованными значениями показателей.
        
        Args:
            calculated_df: DataFrame с расчетными данными
            config_manager: Менеджер конфигурации
        
        Returns:
            DataFrame с нормализованными значениями показателей
        """
        self.logger.info("=== Начало нормализации показателей (вариант 3) ===", "FileProcessor", "_normalize_indicators")
        
        # Базовые колонки
        base_columns = ["Табельный", "ТБ", "ГОСБ", "ФИО"]
        
        # Группируем колонки по месяцам и группам
        month_data = {}  # {month: {"OD": col_name, "RA": col_name, "PS": col_name}}
        
        for col in calculated_df.columns:
            if col in base_columns:
                continue
            
            match = re.search(r'^([A-Z]+)\s+\(M-(\d{1,2})\)', col)
            if match:
                group = match.group(1)
                month = int(match.group(2))
                
                if month not in month_data:
                    month_data[month] = {}
                month_data[month][group] = col
        
        # Создаем DataFrame для нормализованных данных
        normalized_df = calculated_df[base_columns].copy()
        
        # Получаем направления для каждого показателя
        od_config = config_manager.get_group_config("OD").defaults if "OD" in config_manager.groups else None
        ra_config = config_manager.get_group_config("RA").defaults if "RA" in config_manager.groups else None
        ps_config = config_manager.get_group_config("PS").defaults if "PS" in config_manager.groups else None
        
        od_direction = od_config.indicator_direction if od_config else "MAX"
        ra_direction = ra_config.indicator_direction if ra_config else "MAX"
        ps_direction = ps_config.indicator_direction if ps_config else "MAX"
        
        # ОПТИМИЗАЦИЯ: Векторизованная нормализация для каждого показателя
        # Для каждого показателя (OD, RA, PS) нормализуем значения по месяцам для каждого КМ
        for group_name, direction in [("OD", od_direction), ("RA", ra_direction), ("PS", ps_direction)]:
            # Собираем все колонки для данного показателя
            group_cols = {}
            for month in sorted(month_data.keys()):
                col = month_data[month].get(group_name)
                if col and col in calculated_df.columns:
                    group_cols[month] = col
            
            if not group_cols:
                continue
            
            # ОПТИМИЗАЦИЯ: Создаем временный DataFrame с данными показателя
            group_data = pd.DataFrame(index=calculated_df.index)
            for month, col in group_cols.items():
                # Используем fillna(0) только для расчета, но сохраняем NaN для проверки
                group_data[f"M-{month}"] = calculated_df[col]
            
            # Нормализуем для каждого КМ (горизонтально по месяцам)
            # Для каждого КМ находим min и max по месяцам (игнорируя NaN)
            group_min = group_data.min(axis=1, skipna=True)
            group_max = group_data.max(axis=1, skipna=True)
            group_range = group_max - group_min
            
            # ОПТИМИЗАЦИЯ: Обрабатываем деление на ноль и одинаковые значения
            # Проверяем количество месяцев с данными (не NaN и не 0) для каждого КМ
            non_zero_count = (group_data.notna() & (group_data != 0)).sum(axis=1)
            mask_zero_range = (group_range < 1e-10) | group_range.isna()  # Все значения одинаковы или разница очень мала или все NaN
            mask_single_month = non_zero_count <= 1  # Только один месяц с данными или все нули/NaN
            
            # Защита от деления на ноль: заменяем нули в group_range на 1
            group_range_safe = group_range.where(~mask_zero_range, 1.0)
            
            # Нормализуем
            for month in sorted(group_cols.keys()):
                norm_col_name = f"{group_name}_norm (M-{month})"
                col_data = group_data[f"M-{month}"]
                
                # ОПТИМИЗАЦИЯ: Векторизованная нормализация с обработкой edge cases
                if direction == "MAX":
                    # Больше = лучше: нормализуем к [0, 1]
                    normalized = (col_data - group_min) / group_range_safe
                else:  # direction == "MIN"
                    # Меньше = лучше: инвертируем нормализацию
                    normalized = (group_max - col_data) / group_range_safe
                
                # Обрабатываем edge cases (векторизованно)
                # ВАЖНО: Сначала обрабатываем случай "только один месяц с данными", 
                # затем случай "все значения одинаковы"
                
                # Случай 1: Только один месяц с данными (не нулями)
                # Месяц с данными получает 1.0, остальные (нули) получают 0.0
                if direction == "MAX":
                    # Для MAX: месяц с максимальным значением (ненулевым) = 1.0, остальные = 0.0
                    # Проверяем, является ли текущий месяц максимальным и ненулевым
                    is_max_and_nonzero = (col_data == group_max) & (col_data != 0) & (non_zero_count == 1)
                    # Для КМ с одним месяцем данных: сначала всем 0, затем максимуму 1.0
                    normalized = normalized.where(~mask_single_month, 0.0)  # Сначала всем 0
                    normalized = normalized.where(~is_max_and_nonzero, 1.0)  # Затем максимуму 1.0
                else:  # direction == "MIN"
                    # Для MIN: месяц с минимальным значением (ненулевым) = 1.0, остальные = 0.0
                    # Проверяем, является ли текущий месяц минимальным ненулевым
                    is_min_and_nonzero = (col_data == group_min) & (col_data != 0) & (non_zero_count == 1)
                    # Для КМ с одним месяцем данных: сначала всем 0, затем минимуму 1.0
                    normalized = normalized.where(~mask_single_month, 0.0)  # Сначала всем 0
                    normalized = normalized.where(~is_min_and_nonzero, 1.0)  # Затем минимуму 1.0
                
                # Случай 2: Все значения одинаковы (включая все нули) - всем 0.5
                # Это применяется только если НЕ случай "один месяц с данными"
                # (mask_zero_range может быть True и для случая "один месяц", поэтому проверяем ~mask_single_month)
                mask_all_same_not_single = mask_zero_range & ~mask_single_month
                normalized = normalized.where(~mask_all_same_not_single, 0.5)
                
                # Защита от выхода за границы [0, 1] (из-за погрешности вычислений)
                normalized = normalized.clip(0.0, 1.0)
                
                normalized_df[norm_col_name] = normalized
        
        self.logger.info(f"Нормализация завершена: {len(normalized_df)} строк, {len(normalized_df.columns)} колонок", "FileProcessor", "_normalize_indicators")
        return normalized_df
    
    def _normalize_with_direction(self, values: Dict[int, float], direction: str) -> Dict[int, float]:
        """
        Нормализует значения с учетом направления.
        
        Args:
            values: Словарь {month: value}
            direction: "MAX" или "MIN"
        
        Returns:
            Словарь {month: normalized_value} в диапазоне [0, 1]
        """
        if len(values) == 0:
            return {}
        
        if len(values) == 1:
            # Только один месяц - возвращаем 0.5 (среднее значение)
            return {month: 0.5 for month in values.keys()}
        
        min_val = min(values.values())
        max_val = max(values.values())
        
        if abs(max_val - min_val) < 1e-10:
            # Все значения одинаковы
            return {month: 0.5 for month in values.keys()}
        
        normalized = {}
        for month, value in values.items():
            if direction == "MAX":
                # Больше = лучше: нормализуем к [0, 1]
                normalized[month] = (value - min_val) / (max_val - min_val)
            else:  # direction == "MIN"
                # Меньше = лучше: инвертируем нормализацию
                normalized[month] = (max_val - value) / (max_val - min_val)
        
        return normalized
    
    def _calculate_best_month_variant3(self, calculated_df: pd.DataFrame, normalized_df: pd.DataFrame, config_manager) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Рассчитывает лучший месяц для каждого КМ на основе нормализованных значений (вариант 3).
        
        Создает листы "Места и выбор" и "Итог".
        
        Args:
            calculated_df: DataFrame с расчетными данными
            normalized_df: DataFrame с нормализованными данными
            config_manager: Менеджер конфигурации
        
        Returns:
            Tuple[DataFrame для "Места и выбор", DataFrame для "Итог"]
        """
        self.logger.info("=== Начало расчета лучшего месяца (вариант 3) ===", "FileProcessor", "_calculate_best_month_variant3")
        
        # Получаем веса и направления
        od_config = config_manager.get_group_config("OD").defaults if "OD" in config_manager.groups else None
        ra_config = config_manager.get_group_config("RA").defaults if "RA" in config_manager.groups else None
        ps_config = config_manager.get_group_config("PS").defaults if "PS" in config_manager.groups else None
        
        weight_od = od_config.weight if od_config else 0.33
        weight_ra = ra_config.weight if ra_config else 0.33
        weight_ps = ps_config.weight if ps_config else 0.34
        
        # Базовые колонки
        base_columns = ["Табельный", "ТБ", "ГОСБ", "ФИО"]
        
        # Группируем колонки по месяцам
        month_data = {}  # {month: {"OD": col_name, "RA": col_name, "PS": col_name}}
        
        for col in calculated_df.columns:
            if col in base_columns:
                continue
            
            match = re.search(r'^([A-Z]+)\s+\(M-(\d{1,2})\)', col)
            if match:
                group = match.group(1)
                month = int(match.group(2))
                
                if month not in month_data:
                    month_data[month] = {}
                month_data[month][group] = col
        
        # Создаем DataFrame для "Места и выбор"
        places_df = normalized_df[base_columns].copy()
        
        # Создаем DataFrame для "Итог"
        final_df = calculated_df[base_columns].copy()
        
        # ОПТИМИЗАЦИЯ: Векторизованный расчет Score для каждого месяца
        score_cols = {}
        for month in sorted(month_data.keys()):
            score = pd.Series(0.0, index=calculated_df.index)
            
            # ОПТИМИЗАЦИЯ: Векторизованный расчет Score
            od_norm_col = f"OD_norm (M-{month})"
            ra_norm_col = f"RA_norm (M-{month})"
            ps_norm_col = f"PS_norm (M-{month})"
            
            if od_norm_col in normalized_df.columns:
                score += normalized_df[od_norm_col].fillna(0) * weight_od
            
            if ra_norm_col in normalized_df.columns:
                score += normalized_df[ra_norm_col].fillna(0) * weight_ra
            
            if ps_norm_col in normalized_df.columns:
                score += normalized_df[ps_norm_col].fillna(0) * weight_ps
            
            score_col_name = f"Score (M-{month})"
            places_df[score_col_name] = score
            score_cols[month] = score_col_name
        
        # ОПТИМИЗАЦИЯ: Векторизованный расчет горизонтального ранга
        # Создаем DataFrame со всеми Score для удобства работы
        score_df = pd.DataFrame(index=calculated_df.index)
        for month, col in score_cols.items():
            score_df[f"M-{month}"] = places_df[col]
        
        # Для каждого КМ рассчитываем ранг (горизонтально)
        # Используем rank с method='min' и ascending=False (больше = лучше)
        # na_option='keep' - NaN остаются NaN
        rank_df = score_df.rank(axis=1, method='min', ascending=False, na_option='keep')
        
        # Добавляем ранги в places_df
        for month in sorted(month_data.keys()):
            rank_col_name = f"Место (M-{month})"
            places_df[rank_col_name] = rank_df[f"M-{month}"].fillna(0).astype(int)
        
        # ОПТИМИЗАЦИЯ: Векторизованный поиск лучшего месяца
        # Находим все месяцы с рангом 1 для каждого КМ
        best_month_series = pd.Series("", index=calculated_df.index, dtype=str)
        
        # Создаем маску для месяцев с рангом 1 (заполняем NaN как False)
        rank_1_mask = (rank_df == 1).fillna(False)
        
        # Для каждого КМ собираем месяцы с рангом 1
        for idx in calculated_df.index:
            best_months = []
            for month in sorted(month_data.keys()):
                col_name = f"M-{month}"
                if col_name in rank_1_mask.columns:
                    if rank_1_mask.loc[idx, col_name]:
                        best_months.append(month)
            
            if best_months:
                best_month_series.loc[idx] = ", ".join([str(m) for m in sorted(best_months)])
                
                # Добавляем значения показателей лучшего месяца в final_df
                best_month = best_months[0]  # Берем первый, если несколько
                od_col = month_data[best_month].get("OD")
                ra_col = month_data[best_month].get("RA")
                ps_col = month_data[best_month].get("PS")
                
                if od_col and od_col in calculated_df.columns:
                    final_df.loc[idx, "OD (лучший месяц)"] = calculated_df.loc[idx, od_col]
                if ra_col and ra_col in calculated_df.columns:
                    final_df.loc[idx, "RA (лучший месяц)"] = calculated_df.loc[idx, ra_col]
                if ps_col and ps_col in calculated_df.columns:
                    final_df.loc[idx, "PS (лучший месяц)"] = calculated_df.loc[idx, ps_col]
        
        # Добавляем колонку "Лучший месяц" в places_df и final_df
        places_df["Лучший месяц"] = best_month_series
        final_df["Лучший месяц"] = best_month_series
        
        self.logger.info(f"Расчет лучшего месяца завершен: определен для {len(best_month_series[best_month_series != ''])} КМ", "FileProcessor", "_calculate_best_month_variant3")
        
        return places_df, final_df
    
    def prepare_statistics_sheet(self) -> Optional[pd.DataFrame]:
        """
        Формирует лист со статистикой обработки данных.
        
        Returns:
            Optional[pd.DataFrame]: DataFrame со статистикой или None, если статистика отключена
        """
        if not ENABLE_STATISTICS:
            return None
        
        self.logger.info("=== Начало формирования листа 'Статистика' ===", "FileProcessor", "prepare_statistics_sheet")
        
        # Создаем список всех таблиц статистики
        statistics_tables = []
        
        # Таблица 1: Общая статистика
        summary_data = []
        summary_data.append(["Параметр", "Значение"])
        summary_data.append(["Всего обработано КМ (табельных номеров)", self.statistics["summary"].get("total_km", 0)])
        summary_data.append(["Всего уникальных клиентов", self.statistics["summary"].get("total_clients", 0)])
        summary_data.append(["", ""])  # Пустая строка для разделения
        
        # Таблица 2: Количество КМ по ТБ
        if "by_tb" in self.statistics["summary"]:
            summary_data.append(["Количество КМ по ТБ", ""])
            summary_data.append(["ТБ", "Количество КМ"])
            for tb, count in sorted(self.statistics["summary"]["by_tb"].items(), key=lambda x: x[1], reverse=True):
                summary_data.append([tb, count])
            summary_data.append(["", ""])  # Пустая строка для разделения
        
        # Таблица 3: Количество КМ по ГОСБ
        if "by_gosb" in self.statistics["summary"]:
            summary_data.append(["Количество КМ по ГОСБ", ""])
            summary_data.append(["ГОСБ", "Количество КМ"])
            for gosb, count in sorted(self.statistics["summary"]["by_gosb"].items(), key=lambda x: x[1], reverse=True):
                summary_data.append([gosb, count])
            summary_data.append(["", ""])  # Пустая строка для разделения
        
        # Таблица 4: Статистика по файлам (исходные строки, удалено, оставлено)
        summary_data.append(["Статистика обработки файлов", ""])
        summary_data.append(["Группа", "Файл", "Исходно строк", "Удалено по drop_rules", "Оставлено по in_rules", "Итогово строк"])
        
        total_initial = 0
        total_dropped = 0
        total_final = 0
        
        for group in sorted(self.statistics["files"].keys()):
            for file_name in sorted(self.statistics["files"][group].keys()):
                file_stats = self.statistics["files"][group][file_name]
                initial = file_stats.get("initial_rows", 0)
                final = file_stats.get("final_rows", 0)
                dropped_count = sum(file_stats.get("dropped_by_rule", {}).values())
                kept_count = sum(file_stats.get("kept_by_rule", {}).values())
                
                summary_data.append([group, file_name, initial, dropped_count, kept_count, final])
                
                total_initial += initial
                total_dropped += dropped_count
                total_final += final
        
        summary_data.append(["ИТОГО", "", total_initial, total_dropped, "", total_final])
        summary_data.append(["", ""])  # Пустая строка для разделения
        
        # Таблица 5: Детальная статистика по drop_rules
        summary_data.append(["Детальная статистика по drop_rules", ""])
        summary_data.append(["Группа", "Файл", "Правило", "Удалено строк"])
        
        for group in sorted(self.statistics["files"].keys()):
            for file_name in sorted(self.statistics["files"][group].keys()):
                file_stats = self.statistics["files"][group][file_name]
                dropped_by_rule = file_stats.get("dropped_by_rule", {})
                for rule, count in sorted(dropped_by_rule.items(), key=lambda x: x[1], reverse=True):
                    if count > 0:
                        summary_data.append([group, file_name, rule, count])
        
        summary_data.append(["", ""])  # Пустая строка для разделения
        
        # Таблица 6: Детальная статистика по in_rules
        summary_data.append(["Детальная статистика по in_rules", ""])
        summary_data.append(["Группа", "Файл", "Правило", "Оставлено строк"])
        
        for group in sorted(self.statistics["files"].keys()):
            for file_name in sorted(self.statistics["files"][group].keys()):
                file_stats = self.statistics["files"][group][file_name]
                kept_by_rule = file_stats.get("kept_by_rule", {})
                for rule, count in sorted(kept_by_rule.items(), key=lambda x: x[1], reverse=True):
                    if count > 0:
                        summary_data.append([group, file_name, rule, count])
        
        summary_data.append(["", ""])  # Пустая строка для разделения
        
        # Таблица 7: Статистика выбора табельных номеров
        summary_data.append(["Статистика выбора табельных номеров", ""])
        summary_data.append(["Группа", "Файл", "Всего вариантов ТБ/ГОСБ", "Выбрано уникальных", "Табельных с несколькими вариантами"])
        
        for group in sorted(self.statistics["tab_selection"].keys()):
            for file_name in sorted(self.statistics["tab_selection"][group].keys()):
                tab_stats = self.statistics["tab_selection"][group][file_name]
                summary_data.append([
                    group,
                    file_name,
                    tab_stats.get("total_variants", 0),
                    tab_stats.get("selected_count", 0),
                    tab_stats.get("variants_with_multiple", 0)
                ])
        
        # Создаем DataFrame
        if len(summary_data) > 0:
            # Находим максимальную длину строки для определения количества колонок
            max_cols = max(len(row) for row in summary_data) if summary_data else 2
            
            # Дополняем все строки до максимальной длины
            summary_data_padded = [row + [""] * (max_cols - len(row)) for row in summary_data]
            
            statistics_df = pd.DataFrame(summary_data_padded)
        else:
            statistics_df = pd.DataFrame([["Статистика недоступна", ""]])
        
        self.logger.info(f"Лист 'Статистика': Подготовлено {len(statistics_df)} строк статистики", "FileProcessor", "prepare_statistics_sheet")
        self.logger.info("=== Завершена подготовка листа 'Статистика' ===", "FileProcessor", "prepare_statistics_sheet")
        
        # Выводим статистику в лог
        self._log_statistics()
        
        return statistics_df
    
    def _log_statistics(self) -> None:
        """Выводит статистику в лог."""
        if not ENABLE_STATISTICS:
            return
        
        self.logger.info("=" * 80, "FileProcessor", "_log_statistics")
        self.logger.info("СТАТИСТИКА ОБРАБОТКИ ДАННЫХ", "FileProcessor", "_log_statistics")
        self.logger.info("=" * 80, "FileProcessor", "_log_statistics")
        
        # Общая статистика
        self.logger.info(f"Всего обработано КМ (табельных номеров): {self.statistics['summary'].get('total_km', 0)}", "FileProcessor", "_log_statistics")
        self.logger.info(f"Всего уникальных клиентов: {self.statistics['summary'].get('total_clients', 0)}", "FileProcessor", "_log_statistics")
        
        # Статистика по ТБ
        if "by_tb" in self.statistics["summary"]:
            self.logger.info("Количество КМ по ТБ:", "FileProcessor", "_log_statistics")
            for tb, count in sorted(self.statistics["summary"]["by_tb"].items(), key=lambda x: x[1], reverse=True):
                self.logger.info(f"  {tb}: {count}", "FileProcessor", "_log_statistics")
        
        # Статистика по файлам
        total_initial = 0
        total_dropped = 0
        total_final = 0
        
        for group in sorted(self.statistics["files"].keys()):
            self.logger.info(f"Группа {group}:", "FileProcessor", "_log_statistics")
            for file_name in sorted(self.statistics["files"][group].keys()):
                file_stats = self.statistics["files"][group][file_name]
                initial = file_stats.get("initial_rows", 0)
                final = file_stats.get("final_rows", 0)
                dropped_count = sum(file_stats.get("dropped_by_rule", {}).values())
                
                self.logger.info(f"  {file_name}: исходно {initial}, удалено {dropped_count}, итого {final}", "FileProcessor", "_log_statistics")
                
                total_initial += initial
                total_dropped += dropped_count
                total_final += final
        
        self.logger.info(f"ИТОГО: исходно {total_initial}, удалено {total_dropped}, итого {total_final}", "FileProcessor", "_log_statistics")
        
        # Статистика выбора табельных
        for group in sorted(self.statistics["tab_selection"].keys()):
            self.logger.info(f"Выбор табельных номеров - группа {group}:", "FileProcessor", "_log_statistics")
            for file_name in sorted(self.statistics["tab_selection"][group].keys()):
                tab_stats = self.statistics["tab_selection"][group][file_name]
                self.logger.info(
                    f"  {file_name}: всего вариантов {tab_stats.get('total_variants', 0)}, "
                    f"выбрано {tab_stats.get('selected_count', 0)}, "
                    f"с несколькими вариантами {tab_stats.get('variants_with_multiple', 0)}",
                    "FileProcessor", "_log_statistics"
                )
        
        self.logger.info("=" * 80, "FileProcessor", "_log_statistics")


# ============================================================================
# МОДУЛЬ ФОРМАТИРОВАНИЯ EXCEL
# ============================================================================

class ExcelFormatter:
    """Класс для форматирования Excel файлов с использованием только базовых модулей Anaconda."""
    
    def __init__(self, logger_instance: Optional[Logger] = None):
        """
        Инициализация форматтера.
        
        Args:
            logger_instance: Экземпляр логгера
        """
        self.min_width = 15
        self.max_width = 150
        self.logger = logger_instance
    
    def _calculate_column_width(self, df: pd.DataFrame, col_name: str) -> float:
        """
        Вычисляет оптимальную ширину колонки на основе содержимого.
        
        Args:
            df: DataFrame с данными
            col_name: Название колонки
            
        Returns:
            float: Ширина колонки
        """
        if col_name not in df.columns:
            return self.min_width
        
        # Максимальная длина в заголовке
        max_length = len(str(col_name))
        
        # Максимальная длина в данных (первые 100 строк для производительности)
        sample_size = min(100, len(df))
        for idx in range(sample_size):
            value = df[col_name].iloc[idx]
            if pd.notna(value):
                max_length = max(max_length, len(str(value)))
        
        # Применяем ограничения
        width = max(self.min_width, min(max_length + 2, self.max_width))
        return width
    
    def create_formatted_excel(self, summary_df: pd.DataFrame, calculated_df: pd.DataFrame, 
                              normalized_df: pd.DataFrame, places_df: pd.DataFrame, final_df: pd.DataFrame,
                              output_path: str, statistics_df: Optional[pd.DataFrame] = None) -> None:
        """
        Создает новый Excel файл с форматированием используя только базовые модули Anaconda.
        Приоритет: openpyxl > xlsxwriter > без форматирования
        
        Создает 5 основных листов + лист "Статистика" (если включен):
        1. "Исходник" - исходные отфильтрованные данные
        2. "Расчет" - расчетные данные (факт, прирост по 2м, прирост по 3м)
        3. "Нормализация" - нормализованные значения показателей
        4. "Места и выбор" - Score, ранги и лучший месяц
        5. "Итог" - итоговые данные с выбором месяца и значениями показателей
        6. "Статистика" - статистика обработки (если ENABLE_STATISTICS = True и statistics_df не None)
        
        Args:
            summary_df: DataFrame с исходными данными (лист "Исходник")
            calculated_df: DataFrame с расчетными данными (лист "Расчет")
            normalized_df: DataFrame с нормализованными данными (лист "Нормализация")
            places_df: DataFrame с Score и рангами (лист "Места и выбор")
            final_df: DataFrame с итоговыми данными (лист "Итог")
            output_path: Путь для сохранения файла
            statistics_df: DataFrame со статистикой (лист "Статистика") или None, если статистика отключена
        """
        self.logger.info(f"Создание форматированного Excel файла {output_path}", "ExcelFormatter", "create_formatted_excel")
        
        try:
            if OPENPYXL_AVAILABLE:
                # Используем openpyxl для форматирования (приоритетный вариант)
                self._create_with_openpyxl(summary_df, calculated_df, normalized_df, places_df, final_df, output_path, statistics_df)
            elif XLSXWRITER_AVAILABLE:
                # Используем xlsxwriter для форматирования (если openpyxl недоступен)
                self._create_with_xlsxwriter(summary_df, calculated_df, normalized_df, places_df, final_df, output_path, statistics_df)
            else:
                # Используем pandas ExcelWriter без форматирования
                self.logger.warning("openpyxl и xlsxwriter недоступны, создается файл без форматирования", "ExcelFormatter", "create_formatted_excel")
                # Пробуем использовать доступный engine
                try:
                    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                        summary_df.to_excel(writer, sheet_name="Исходник", index=False)
                        calculated_df.to_excel(writer, sheet_name="Расчет", index=False)
                        normalized_df.to_excel(writer, sheet_name="Нормализация", index=False)
                        places_df.to_excel(writer, sheet_name="Места и выбор", index=False)
                        final_df.to_excel(writer, sheet_name="Итог", index=False)
                        if statistics_df is not None:
                            statistics_df.to_excel(writer, sheet_name="Статистика", index=False, header=False)
                except:
                    try:
                        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                            summary_df.to_excel(writer, sheet_name="Исходник", index=False)
                            calculated_df.to_excel(writer, sheet_name="Расчет", index=False)
                            normalized_df.to_excel(writer, sheet_name="Нормализация", index=False)
                            places_df.to_excel(writer, sheet_name="Места и выбор", index=False)
                            final_df.to_excel(writer, sheet_name="Итог", index=False)
                            if statistics_df is not None:
                                statistics_df.to_excel(writer, sheet_name="Статистика", index=False, header=False)
                    except:
                        # Если не получилось, используем любой доступный engine
                        with pd.ExcelWriter(output_path) as writer:
                            summary_df.to_excel(writer, sheet_name="Исходник", index=False)
                            calculated_df.to_excel(writer, sheet_name="Расчет", index=False)
                            normalized_df.to_excel(writer, sheet_name="Нормализация", index=False)
                            places_df.to_excel(writer, sheet_name="Места и выбор", index=False)
                            final_df.to_excel(writer, sheet_name="Итог", index=False)
                            if statistics_df is not None:
                                statistics_df.to_excel(writer, sheet_name="Статистика", index=False, header=False)
                self.logger.info(f"Файл {output_path} создан без форматирования", "ExcelFormatter", "create_formatted_excel")
            
        except Exception as e:
            self.logger.error(f"Ошибка при создании Excel файла {output_path}: {str(e)}", "ExcelFormatter", "create_formatted_excel")
            # Пробуем создать без форматирования
            try:
                with pd.ExcelWriter(output_path) as writer:
                    summary_df.to_excel(writer, sheet_name="Исходник", index=False)
                    calculated_df.to_excel(writer, sheet_name="Расчет", index=False)
                    normalized_df.to_excel(writer, sheet_name="Нормализация", index=False)
                    places_df.to_excel(writer, sheet_name="Места и выбор", index=False)
                    final_df.to_excel(writer, sheet_name="Итог", index=False)
                    if statistics_df is not None:
                        statistics_df.to_excel(writer, sheet_name="Статистика", index=False, header=False)
                self.logger.warning(f"Файл создан без форматирования из-за ошибки: {str(e)}", "ExcelFormatter", "create_formatted_excel")
            except Exception as e2:
                self.logger.error(f"Критическая ошибка при создании файла: {str(e2)}", "ExcelFormatter", "create_formatted_excel")
                raise
    
    def _create_with_openpyxl(self, summary_df: pd.DataFrame, calculated_df: pd.DataFrame,
                             normalized_df: pd.DataFrame, places_df: pd.DataFrame, final_df: pd.DataFrame,
                             output_path: str, statistics_df: Optional[pd.DataFrame] = None) -> None:
        """
        Создает Excel файл с форматированием используя openpyxl.
        
        Args:
            summary_df: DataFrame с исходными данными
            calculated_df: DataFrame с расчетными данными
            normalized_df: DataFrame с нормализованными данными
            places_df: DataFrame с Score и рангами
            final_df: DataFrame с итоговыми данными
            output_path: Путь для сохранения файла
        """
        self.logger.info(f"Использование openpyxl для форматирования", "ExcelFormatter", "_create_with_openpyxl")
        
        # Сначала сохраняем DataFrame в Excel через pandas
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name="Исходник", index=False)
            calculated_df.to_excel(writer, sheet_name="Расчет", index=False)
            normalized_df.to_excel(writer, sheet_name="Нормализация", index=False)
            places_df.to_excel(writer, sheet_name="Места и выбор", index=False)
            final_df.to_excel(writer, sheet_name="Итог", index=False)
            if statistics_df is not None:
                statistics_df.to_excel(writer, sheet_name="Статистика", index=False, header=False)
        
        # Теперь форматируем файл
        wb = load_workbook(output_path)
        
        # Форматируем все листы
        sheet_data = {
            "Исходник": summary_df,
            "Расчет": calculated_df,
            "Нормализация": normalized_df,
            "Места и выбор": places_df,
            "Итог": final_df
        }
        
        if statistics_df is not None:
            sheet_data["Статистика"] = statistics_df
        
        for sheet_name, df in sheet_data.items():
            if sheet_name not in wb.sheetnames:
                continue
            
            ws = wb[sheet_name]
            if sheet_name == "Статистика":
                # Для листа статистики используем специальное форматирование
                self._format_statistics_sheet_openpyxl(ws, df)
            else:
                self._format_sheet_openpyxl(ws, df)
        
        # Сохраняем файл
        wb.save(output_path)
        self.logger.info(f"Файл {output_path} успешно создан с форматированием (openpyxl)", "ExcelFormatter", "_create_with_openpyxl")
    
    def _format_sheet_openpyxl(self, ws, df: pd.DataFrame) -> None:
        """
        Форматирует лист Excel используя openpyxl.
        
        Args:
            ws: Рабочий лист openpyxl
            df: DataFrame с данными
        """
        
        # Фиксируем первую строку и 4 колонку (после ФИО)
        # freeze_panes использует адрес ячейки, начиная с которой будет прокрутка
        # E2 означает: строка 2 (заголовок зафиксирован), колонка E (5-я, после ФИО)
        ws.freeze_panes = "E2"
        self.logger.debug("Первая строка и 4 колонка зафиксированы", "ExcelFormatter", "_format_sheet_openpyxl")
        
        # Форматируем заголовки (первая строка)
        header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
        header_font = Font(bold=True, size=12)
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        
        for cell in ws[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
        
        self.logger.debug("Заголовки отформатированы", "ExcelFormatter", "_create_with_openpyxl")
        
        # Настраиваем ширину колонок
        for col_idx, column in enumerate(ws.iter_cols(min_row=1, max_row=1), start=1):
            col_letter = get_column_letter(col_idx)
            
            # Вычисляем оптимальную ширину на основе содержимого
            max_length = 0
            for cell in column:
                if cell.value:
                    cell_value = str(cell.value)
                    max_length = max(max_length, len(cell_value))
            
            # Учитываем содержимое всех ячеек в колонке (первые 100 строк для производительности)
            for row in ws.iter_rows(min_row=2, max_row=min(102, ws.max_row), min_col=col_idx, max_col=col_idx):
                for cell in row:
                    if cell.value:
                        cell_value = str(cell.value)
                        max_length = max(max_length, len(cell_value))
            
            # Применяем ограничения
            width = max(self.min_width, min(max_length + 2, self.max_width))
            ws.column_dimensions[col_letter].width = width
            
            self.logger.debug(f"Колонка {col_letter} установлена ширина {width}", "ExcelFormatter", "_create_with_openpyxl")
        
        # Настраиваем выравнивание и форматирование для всех ячеек
        # Определяем базовые колонки (текстовые)
        base_columns = ["Табельный", "ТБ", "ГОСБ", "ФИО"]
        
        # Формат для чисел: разделитель разрядов и два знака после запятой
        number_format = "#,##0.00"
        # Формат для рангов: целое число с разделителем разрядов (без дробной части)
        rank_format = "#,##0"
        # Текстовый формат для сохранения лидирующих нулей
        text_format = "@"
        
        for row_idx, row in enumerate(ws.iter_rows(min_row=2), start=2):
            for col_idx, cell in enumerate(row, start=1):
                col_name = ws.cell(row=1, column=col_idx).value
                
                # Если это колонка "Табельный" - текстовый формат для сохранения лидирующих нулей
                if col_name == "Табельный":
                    cell.number_format = text_format
                    cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                # Если это другие базовые колонки - текстовое выравнивание
                elif col_name in base_columns:
                    cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                # Если это колонка Score - число с разделителем разрядов и двумя знаками после запятой
                elif col_name and col_name.startswith("Score"):
                    if pd.notna(cell.value) and isinstance(cell.value, (int, float)):
                        cell.number_format = number_format
                        cell.alignment = Alignment(horizontal="right", vertical="center")
                    else:
                        cell.alignment = Alignment(horizontal="right", vertical="center")
                # Если это колонка нормализованных значений - число с разделителем разрядов и двумя знаками после запятой
                elif col_name and "_norm" in col_name:
                    if pd.notna(cell.value) and isinstance(cell.value, (int, float)):
                        cell.number_format = number_format
                        cell.alignment = Alignment(horizontal="right", vertical="center")
                    else:
                        cell.alignment = Alignment(horizontal="right", vertical="center")
                # Если это колонка "Место" - целое число с разделителем разрядов
                elif col_name and col_name.startswith("Место"):
                    if pd.notna(cell.value) and isinstance(cell.value, (int, float)):
                        cell.number_format = rank_format
                        cell.alignment = Alignment(horizontal="right", vertical="center")
                    else:
                        cell.alignment = Alignment(horizontal="right", vertical="center")
                # Если это колонка "Лучший месяц" - текстовое выравнивание
                elif col_name == "Лучший месяц":
                    cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                else:
                    # Для числовых колонок - числовой формат и выравнивание по правому краю
                    if pd.notna(cell.value) and isinstance(cell.value, (int, float)):
                        cell.number_format = number_format
                        cell.alignment = Alignment(horizontal="right", vertical="center")
                    else:
                        cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        
        self.logger.debug("Выравнивание и форматирование настроены", "ExcelFormatter", "_format_sheet_openpyxl")
        
        # Включаем автофильтр
        ws.auto_filter.ref = ws.dimensions
        self.logger.debug("Автофильтр включен", "ExcelFormatter", "_format_sheet_openpyxl")
    
    def _format_statistics_sheet_openpyxl(self, ws, df: pd.DataFrame) -> None:
        """
        Форматирует лист статистики используя openpyxl.
        
        Args:
            ws: Рабочий лист openpyxl
            df: DataFrame со статистикой
        """
        # Фиксируем первую строку
        ws.freeze_panes = "A2"
        
        # Форматируем заголовки разделов (строки с одним значением в первой колонке и пустой второй)
        section_fill = PatternFill(start_color="E6E6FA", end_color="E6E6FA", fill_type="solid")
        section_font = Font(bold=True, size=14, color="000080")
        section_alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        
        # Форматируем заголовки таблиц (строки с двумя значениями, где второе пустое)
        table_header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
        table_header_font = Font(bold=True, size=11)
        table_header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        
        # Обычный текст
        text_alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        number_alignment = Alignment(horizontal="right", vertical="center")
        
        for row_idx, row in enumerate(ws.iter_rows(min_row=1), start=1):
            for col_idx, cell in enumerate(row):
                if cell.value is None:
                    continue
                
                value = str(cell.value)
                # Проверяем, является ли это заголовком раздела (первая колонка заполнена, вторая пустая)
                if col_idx == 0 and len(row) > 1:
                    next_cell_value = row[1].value if len(row) > 1 else None
                    if next_cell_value is None or str(next_cell_value).strip() == "":
                        # Это заголовок раздела
                        cell.font = section_font
                        cell.fill = section_fill
                        cell.alignment = section_alignment
                    elif col_idx == 0 and row_idx == 1:
                        # Первая строка - заголовок таблицы
                        cell.font = table_header_font
                        cell.fill = table_header_fill
                        cell.alignment = table_header_alignment
                    else:
                        # Обычный текст
                        cell.alignment = text_alignment
                elif col_idx == 1 and row_idx == 1:
                    # Вторая колонка первой строки - заголовок таблицы
                    cell.font = table_header_font
                    cell.fill = table_header_fill
                    cell.alignment = table_header_alignment
                else:
                    # Проверяем, является ли значение числом
                    try:
                        num_value = float(value)
                        cell.alignment = number_alignment
                        cell.number_format = "#,##0"
                    except (ValueError, TypeError):
                        cell.alignment = text_alignment
        
        # Настраиваем ширину колонок
        for col_idx, column in enumerate(ws.iter_cols(min_row=1, max_row=min(100, ws.max_row)), start=1):
            col_letter = get_column_letter(col_idx)
            max_length = 0
            for cell in column:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            width = max(15, min(max_length + 2, 100))
            ws.column_dimensions[col_letter].width = width
        
        self.logger.debug("Лист 'Статистика' отформатирован", "ExcelFormatter", "_format_statistics_sheet_openpyxl")
    
    def _create_with_xlsxwriter(self, summary_df: pd.DataFrame, calculated_df: pd.DataFrame,
                                normalized_df: pd.DataFrame, places_df: pd.DataFrame, final_df: pd.DataFrame,
                                output_path: str, statistics_df: Optional[pd.DataFrame] = None) -> None:
        """
        Создает Excel файл с форматированием используя xlsxwriter.
        
        Args:
            summary_df: DataFrame с исходными данными
            calculated_df: DataFrame с расчетными данными
            normalized_df: DataFrame с нормализованными данными
            places_df: DataFrame с Score и рангами
            final_df: DataFrame с итоговыми данными
            output_path: Путь для сохранения файла
        """
        # Создаем рабочую книгу
        workbook = xlsxwriter.Workbook(output_path)

        # Создаем листы
        worksheet_summary = workbook.add_worksheet("Исходник")
        worksheet_calc = workbook.add_worksheet("Расчет")
        worksheet_norm = workbook.add_worksheet("Нормализация")
        worksheet_places = workbook.add_worksheet("Места и выбор")
        worksheet_final = workbook.add_worksheet("Итог")
        
        # Форматируем все листы
        self._format_sheet_xlsxwriter(workbook, worksheet_summary, summary_df)
        self._format_sheet_xlsxwriter(workbook, worksheet_calc, calculated_df)
        self._format_sheet_xlsxwriter(workbook, worksheet_norm, normalized_df)
        self._format_sheet_xlsxwriter(workbook, worksheet_places, places_df)
        self._format_sheet_xlsxwriter(workbook, worksheet_final, final_df)
        
        if statistics_df is not None:
            worksheet_statistics = workbook.add_worksheet("Статистика")
            # Записываем данные в лист статистики
            for row_idx, (_, row) in enumerate(statistics_df.iterrows(), start=0):
                for col_idx, value in enumerate(row):
                    if pd.notna(value):
                        worksheet_statistics.write(row_idx, col_idx, value)
            # Форматируем лист статистики
            self._format_statistics_sheet_xlsxwriter(workbook, worksheet_statistics, statistics_df)

        # Закрываем рабочую книгу
        workbook.close()
        
        self.logger.info(f"Файл {output_path} успешно создан с форматированием (xlsxwriter)", "ExcelFormatter", "_create_with_xlsxwriter")
    
    def _format_sheet_xlsxwriter(self, workbook, worksheet, df: pd.DataFrame) -> None:
        """
        Форматирует лист Excel используя xlsxwriter.
        
        Args:
            workbook: Рабочая книга xlsxwriter
            worksheet: Рабочий лист xlsxwriter
            df: DataFrame с данными
        """
        
        # Формат для заголовков
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 12,
            'align': 'center',
            'valign': 'vcenter',
            'text_wrap': True,
            'bg_color': '#D3D3D3'
        })
        
        # Формат для текстовых данных
        text_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'text_wrap': True
        })
        
        # Формат для колонки "Табельный": текстовый формат для сохранения лидирующих нулей
        tab_number_format = workbook.add_format({
            'num_format': '@',  # Текстовый формат
            'align': 'left',
            'valign': 'vcenter'
        })
        
        # Формат для числовых данных: разделитель разрядов и два знака после запятой
        number_format = workbook.add_format({
            'num_format': '#,##0.00',
            'align': 'right',
            'valign': 'vcenter'
        })
        
        # Формат для рангов: целое число с разделителем разрядов (без дробной части)
        rank_format = workbook.add_format({
            'num_format': '#,##0',
            'align': 'right',
            'valign': 'vcenter'
        })
        
        # Определяем базовые колонки (текстовые)
        base_columns = ["Табельный", "ТБ", "ГОСБ", "ФИО"]
        
        # Записываем заголовки
        for col_idx, col_name in enumerate(df.columns):
            worksheet.write(0, col_idx, col_name, header_format)
            # Вычисляем и устанавливаем ширину колонки
            width = self._calculate_column_width(df, col_name)
            worksheet.set_column(col_idx, col_idx, width)
        
        # Записываем данные
        for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
            for col_idx, value in enumerate(row):
                col_name = df.columns[col_idx]
                
                # Выбираем формат в зависимости от типа колонки
                if col_name == "Табельный":
                    # Колонка "Табельный": текстовый формат для сохранения лидирующих нулей
                    if pd.notna(value):
                        worksheet.write(row_idx, col_idx, str(value), tab_number_format)
                    else:
                        worksheet.write(row_idx, col_idx, '', tab_number_format)
                elif col_name in base_columns:
                    # Другие текстовые колонки
                    if pd.notna(value):
                        worksheet.write(row_idx, col_idx, value, text_format)
                    else:
                        worksheet.write(row_idx, col_idx, '', text_format)
                elif col_name and col_name.startswith("Score"):
                    # Колонка Score: число с разделителем разрядов и двумя знаками после запятой
                    if pd.notna(value) and isinstance(value, (int, float)):
                        worksheet.write(row_idx, col_idx, value, number_format)
                    else:
                        worksheet.write(row_idx, col_idx, 0, number_format)
                elif col_name and "_norm" in col_name:
                    # Колонка нормализованных значений: число с разделителем разрядов и двумя знаками после запятой
                    if pd.notna(value) and isinstance(value, (int, float)):
                        worksheet.write(row_idx, col_idx, value, number_format)
                    else:
                        worksheet.write(row_idx, col_idx, 0, number_format)
                elif col_name and col_name.startswith("Место"):
                    # Колонка "Место": целое число с разделителем разрядов
                    if pd.notna(value) and isinstance(value, (int, float)):
                        worksheet.write(row_idx, col_idx, int(value), rank_format)
                    else:
                        worksheet.write(row_idx, col_idx, 0, rank_format)
                elif col_name == "Лучший месяц":
                    # Колонка "Лучший месяц": текстовый формат
                    if pd.notna(value):
                        worksheet.write(row_idx, col_idx, str(value), text_format)
                    else:
                        worksheet.write(row_idx, col_idx, '', text_format)
                else:
                    # Числовые колонки
                    if pd.notna(value) and isinstance(value, (int, float)):
                        worksheet.write(row_idx, col_idx, value, number_format)
                    elif pd.notna(value):
                        worksheet.write(row_idx, col_idx, value, text_format)
                    else:
                        worksheet.write(row_idx, col_idx, 0, number_format)
        
        # Фиксируем первую строку и 4 колонку (после ФИО)
        # freeze_panes(row, col) - строка 1 (индекс 1), колонка 4 (индекс 4, после ФИО)
        worksheet.freeze_panes(1, 4)
        
        # Включаем автофильтр
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
    
    def _format_statistics_sheet_xlsxwriter(self, workbook, worksheet, df: pd.DataFrame) -> None:
        """
        Форматирует лист статистики используя xlsxwriter.
        
        Args:
            workbook: Рабочая книга xlsxwriter
            worksheet: Рабочий лист xlsxwriter
            df: DataFrame со статистикой
        """
        # Формат для заголовков разделов
        section_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'bg_color': '#E6E6FA',
            'font_color': '#000080',
            'align': 'left',
            'valign': 'vcenter',
            'text_wrap': True
        })
        
        # Формат для заголовков таблиц
        table_header_format = workbook.add_format({
            'bold': True,
            'font_size': 11,
            'bg_color': '#D3D3D3',
            'align': 'center',
            'valign': 'vcenter',
            'text_wrap': True
        })
        
        # Формат для текста
        text_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'text_wrap': True
        })
        
        # Формат для чисел
        number_format = workbook.add_format({
            'num_format': '#,##0',
            'align': 'right',
            'valign': 'vcenter'
        })
        
        # Перезаписываем данные с форматированием
        for row_idx, (_, row) in enumerate(df.iterrows(), start=0):
            for col_idx, value in enumerate(row):
                if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
                    continue
                
                # Определяем формат
                cell_format = text_format
                
                # Проверяем, является ли это заголовком раздела (первая колонка заполнена, вторая пустая)
                if col_idx == 0 and len(row) > 1:
                    next_value = row.iloc[col_idx + 1] if col_idx + 1 < len(row) else None
                    if pd.isna(next_value) or (isinstance(next_value, str) and str(next_value).strip() == ""):
                        # Это заголовок раздела
                        cell_format = section_format
                    elif row_idx == 0:
                        # Первая строка - заголовок таблицы
                        cell_format = table_header_format
                elif col_idx > 0 and row_idx == 0:
                    # Первая строка, не первая колонка - заголовок таблицы
                    cell_format = table_header_format
                else:
                    # Проверяем, является ли значение числом
                    try:
                        if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace(',', '').replace('.', '').isdigit()):
                            num_value = float(str(value).replace(',', ''))
                            cell_format = number_format
                        else:
                            cell_format = text_format
                    except (ValueError, TypeError):
                        cell_format = text_format
                
                worksheet.write(row_idx, col_idx, value, cell_format)
        
        # Настраиваем ширину колонок
        for col_idx in range(len(df.columns)):
            max_length = 0
            for row_idx in range(min(100, len(df))):
                value = df.iloc[row_idx, col_idx]
                if pd.notna(value):
                    max_length = max(max_length, len(str(value)))
            width = max(15, min(max_length + 2, 100))
            worksheet.set_column(col_idx, col_idx, width)
        
        self.logger.debug("Лист 'Статистика' отформатирован (xlsxwriter)", "ExcelFormatter", "_format_statistics_sheet_xlsxwriter")


# ============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================================

def main():
    """Основная функция приложения."""
    # Инициализируем логгер
    logger = Logger(log_dir=LOG_DIR, level=LOG_LEVEL, theme=LOG_THEME)
    
    logger.info("=" * 80, "main", "main")
    logger.info("Запуск обработки месячных данных", "main", "main")
    logger.info("=" * 80, "main", "main")
    
    try:
        # Создаем выходной каталог, если его нет
        output_path = Path(OUTPUT_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Входной каталог: {INPUT_DIR}", "main", "main")
        logger.info(f"Выходной каталог: {OUTPUT_DIR}", "main", "main")
        
        # Инициализируем процессор файлов
        processor = FileProcessor(input_dir=INPUT_DIR, logger_instance=logger)
        
        # Загружаем все файлы
        logger.info("Этап 1: Загрузка файлов", "main", "main")
        processor.load_all_files()
        
        # Собираем уникальные табельные номера
        logger.info("Этап 2: Сбор уникальных табельных номеров", "main", "main")
        processor.collect_unique_tab_numbers()
        
        # Подготавливаем сводные данные
        logger.info("Этап 3: Подготовка сводных данных", "main", "main")
        summary_df = processor.prepare_summary_data()
        
        if summary_df.empty:
            logger.error("Сводные данные пусты, обработка завершена", "main", "main")
            return
        
        # Подготавливаем расчетные данные
        logger.info("Этап 4: Подготовка расчетных данных", "main", "main")
        calculated_df = processor.prepare_calculated_data(summary_df)
        
        # Создаем менеджер конфигурации
        config_manager = ConfigManager()
        
        # Нормализуем показатели (вариант 3)
        logger.info("Этап 5: Нормализация показателей", "main", "main")
        normalized_df = processor._normalize_indicators(calculated_df, config_manager)
        
        # Рассчитываем лучший месяц (вариант 3)
        logger.info("Этап 6: Расчет лучшего месяца", "main", "main")
        places_df, final_df = processor._calculate_best_month_variant3(calculated_df, normalized_df, config_manager)
        
        # Подготавливаем лист статистики (если включен)
        statistics_df = None
        if ENABLE_STATISTICS:
            logger.info("Этап 7: Подготовка статистики", "main", "main")
            statistics_df = processor.prepare_statistics_sheet()
        
        # Формируем имя выходного файла с датой и временем
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"Сводные_данные_{timestamp}.xlsx"
        
        # Создаем форматтер
        formatter = ExcelFormatter(logger_instance=logger)
        
        # Сохраняем данные в Excel с форматированием (5 основных листов + статистика, если включена)
        logger.info(f"Этап 8: Сохранение результата в {output_file}", "main", "main")
        formatter.create_formatted_excel(summary_df, calculated_df, normalized_df, places_df, final_df, str(output_file), statistics_df)
        
        if ENABLE_STATISTICS and statistics_df is not None:
            logger.info("Лист 'Статистика' добавлен в файл", "main", "main")
        
        logger.info("=" * 80, "main", "main")
        logger.info(f"Обработка завершена успешно. Результат сохранен в: {output_file}", "main", "main")
        logger.info(f"Обработано табельных номеров: {len(summary_df)}", "main", "main")
        logger.info(f"Колонок в результате: {len(summary_df.columns)}", "main", "main")
        logger.info("=" * 80, "main", "main")
        
    except Exception as e:
        logger.error(f"Критическая ошибка при выполнении: {str(e)}", "main", "main")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}", "main", "main")
        sys.exit(1)


if __name__ == "__main__":
    main()

